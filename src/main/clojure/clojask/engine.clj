(ns clojask.engine
  "The thread-pool engine that runs every compute: one input, num-work
   identical workers and one output, all in this JVM. It replaced Onyx in
   clojask 2.1."
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojask.api.aggregate :refer [start]]
            [clojask.groupby :as groupby]
            [clojask.join :as join]
            [clojask.utils :as u])
  (:import [com.clojask.exception ExecutionException]
           [java.util.concurrent ArrayBlockingQueue Callable ExecutorService
            Executors Future ThreadFactory TimeUnit]))

(defn- thread-factory
  "Daemon threads named prefix-1, prefix-2, ..., so a job that is never
   shut down cannot keep the JVM alive."
  [prefix]
  (let [n (atom 0)]
    (reify ThreadFactory
      (newThread [_ r]
        (doto (Thread. ^Runnable r (str prefix "-" (swap! n inc)))
          (.setDaemon true))))))

(defn- feed
  "Submit (work batch) to pool for every batch, in order, and put the
   futures on queue. batches is not held anywhere else, so a lazy input is
   not retained as it is consumed."
  [^ArrayBlockingQueue queue ^ExecutorService pool work batches]
  (doseq [batch batches]
    (.put queue (.submit pool ^Callable (fn [] (work batch))))))

(defn run
  "Run one job and return nil once every result has been handed to sink.

   open returns {:data batches :close f}, :close optional. A reader thread
   walks batches and submits (work batch) to a pool of num-work threads;
   the calling thread hands the results to sink in input order. At most
   2 * num-work + 2 batches are in flight. work must return realized data,
   otherwise the work moves to the calling thread.

   The first exception from open, the batches, work or sink is rethrown
   unwrapped, after the threads are stopped and the input is closed."
  [num-work open work sink]
  (let [pool (Executors/newFixedThreadPool num-work (thread-factory "clojask-worker"))
        reader-pool (Executors/newSingleThreadExecutor (thread-factory "clojask-reader"))
        queue (ArrayBlockingQueue. (* 2 num-work))
        end (Object.)]
    (try
      (.submit reader-pool
               ^Callable (fn []
                           (try
                             (let [{:keys [data close]} (open)]
                               (try
                                 (feed queue pool work data)
                                 (finally
                                   (when close (close)))))
                             (.put queue end)
                             ;; the calling thread has given up on the job
                             (catch InterruptedException _)
                             (catch Throwable t
                               (.put queue t)))))
      (loop []
        (let [item (.take queue)]
          (cond
            (identical? item end) nil
            (instance? Throwable item) (throw item)
            :else (do (sink (try
                              (.get ^Future item)
                              (catch java.util.concurrent.ExecutionException e
                                (throw (.getCause e)))))
                      (recur)))))
      (finally
        (.shutdownNow pool)
        (.shutdownNow reader-pool)
        ;; let the reader run its finally, so the input is closed on return
        (.awaitTermination reader-pool 10 TimeUnit/SECONDS)))))

(defn- stage-error
  [stage ^Throwable e]
  (ExecutionException. (format "[%s] (original error: %s)" stage (.getMessage e)) e))

(defn- row-worker
  "The work on one batch of rows: the columns at index of every row that
   passes the filters, evaluated and, if format? is true, formatted. Rows
   that fail a filter are dropped."
  [df exception index format?]
  (let [col-info (:col-info df)
        operations (.getDesc col-info)
        types (.getType col-info)
        formats (.getFormatter col-info)
        filters (.getFilters (:row-info df))
        index (vec index)
        eval-res (if exception u/eval-res u/eval-res-ne)
        value (if format?
                (fn [row i] ((or (get formats i) str) (eval-res row types formats operations i)))
                (fn [row i] (eval-res row types formats operations i)))]
    (fn [batch]
      (into []
            (keep (fn [row]
                    (when (u/filter-check filters types row)
                      (mapv (fn [i] (value row i)) index))))
            batch))))

(defn- row-batches
  "An open for run: the rows of df in batches of batch-size."
  [df batch-size]
  (fn []
    (let [{:keys [data close]} ((.getFunc df) :map)]
      {:data (partition-all batch-size data) :close close})))

(defn- prepare
  "(f), with a failure tagged as the preparing stage of stage."
  [stage f]
  (try
    (f)
    (catch Throwable e
      (throw (stage-error (str "preparing stage (" stage ")") e)))))

(defn- run-stage
  "(f), with a failure tagged as the run stage of stage."
  [stage f]
  (try
    (f)
    (catch Throwable e
      (throw (stage-error (str "run stage (" stage ")") e)))))

(defn start-compute
  "Compute df into dist, appending after
   the header that DataFrame.compute may have written. Output is always in
   input order."
  [num-work batch-size df dist exception index melt out]
  (let [work (prepare "compute" #(row-worker df exception index true))]
    (run-stage "compute"
               #(with-open [wtr (io/writer dist :append true)]
                  (run num-work (row-batches df batch-size) work
                       (fn [rows]
                         (doseq [row rows]
                           (out wtr (melt row)))))))
    "success"))

(defn start-aggre-only
  "Fold the rows that pass the
   filters through aggre-func, pairs of a function and a position in
   index, then append the result rows to dist. Rows are folded in input
   order."
  [num-work batch-size df dist exception aggre-func index select out]
  (let [work (prepare "aggregate" #(row-worker df exception index false))
        memo (volatile! (vec (repeat (count aggre-func) start)))]
    (run-stage "aggregate"
               (fn []
                 (run num-work (row-batches df batch-size) work
                      (fn [rows]
                        (doseq [row rows]
                          (vreset! memo (mapv (fn [[f i] prev] (f prev (nth row i))) aggre-func @memo)))))
                 ;; a memo still holding the start sentinel saw no rows: write an empty cell
                 (let [data (mapv (fn [m] (cond (identical? m start) [nil] (coll? m) m :else [m])) @memo)]
                   (when-not (apply = (map count data))
                     (throw (ExecutionException. "aggregation result is not of the same length")))
                   (with-open [wtr (io/writer dist :append true)]
                     (doseq [row (apply map vector data)]
                       (out wtr [(u/gets row select)]))))))
    "success"))

(defn start-groupby
  "Add every row that passes the
   filters to its group, a file under dist when dist is a directory path,
   otherwise the in-memory MGroup dist."
  [num-work batch-size df dist groupby-keys groupby-index exception & {:keys [format] :or {format false}}]
  (let [col-info (:col-info df)
        key-index (.getKeyIndex col-info)
        formatter (.getFormatter col-info)
        work (prepare "groupby" #(row-worker df exception (range (count key-index)) false))]
    (run-stage "groupby"
               #(run num-work (row-batches df batch-size) work
                     (fn [rows]
                       (doseq [row rows]
                         (groupby/output-groupby dist row groupby-keys key-index formatter groupby-index format)))))
    "success"))

(defn- group-worker
  "The work on one group of a group-aggregate, {:file f :d key}: the
   group's result rows, the formatted key followed by the aggregates and
   reordered by index. f is a group file, or the key in source when the
   groups are in memory."
  [df aggre-funcs index formatters source]
  (let [groupby-index (mapv #(nth % 1) (.getGroupbyKeys (:row-info df)))
        key-format (u/formatters-by-position (.getFormatter (:col-info df)) groupby-index)
        key-index (range (count groupby-index))]
    (fn [{:keys [file d]}]
      (let [data (if (nil? source) (groupby/read-csv-seq file) (.getKey source file))
            pre (u/gets-format d key-index key-format)
            cols (zipmap (range) (apply map vector data))
            res (reduce (fn [res [func i]]
                          (let [new (func (get cols i))
                                new (if (coll? new) new [new])
                                new (mapv (fn [v] (if-let [formatter (get formatters i)] (formatter v) (str v))) new)]
                            (if (or (= res []) (= (count new) (count (peek res))))
                              (conj res new)
                              (throw (Exception. "aggregation result is not of the same length")))))
                        []
                        aggre-funcs)]
        (if (= res [])
          [pre]
          (mapv (fn [row] (u/gets (concat pre row) index)) (apply map vector res)))))))

(defn- groups
  "An open for run: one {:file f :d key} per group, from source when the
   groups are in memory, else from the group files."
  [source]
  (fn []
    {:data (if (nil? source)
             (map (fn [file] {:file file :d (read-string (u/decode-str (.getName file)))})
                  (rest (file-seq (io/file "./.clojask/grouped"))))
             (map (fn [key] {:file key :d (read-string key)})
                  (.getKeys source)))}))

(defn start-aggre
  "Aggregate every group on the pool and append the result rows to dist,
   in the order the groups are listed."
  [num-work batch-size df source dist exception aggre-func index formatter out]
  (let [work (prepare "groupby aggregate" #(group-worker df aggre-func index formatter source))]
    (run-stage "groupby aggregate"
               #(with-open [wtr (io/writer dist :append true)]
                  (run num-work (groups source) work
                       (fn [rows]
                         (out wtr rows)))))
    "success"))

(defn start-join
  "Join every row of a that passes the
   filters to its matches in b, whose groups are in source when they are in
   memory, else in the group files under .clojask/join/b/, and append the
   result rows to dist in a's order."
  [num-work batch-size a b source dist exception a-keys b-keys a-roll b-roll join-type limit a-index b-index b-format write-index out]
  (let [a-map (.getKeyIndex (:col-info a))
        work (prepare "join" #(row-worker a exception (range (count a-map)) false))
        output-join (join/join-func join-type (or limit (fn [_ _] true)) source)
        a-format (.getFormatter (:col-info a))
        b-count (count (.getKeyIndex (:col-info b)))]
    (run-stage "join"
               #(with-open [wtr (io/writer dist :append true)]
                  (run num-work (row-batches a batch-size) work
                       (fn [rows]
                         (doseq [row rows]
                           (output-join wtr row a-keys a-map b-keys b-count a-roll b-roll a-format b-format a-index b-index write-index out))))))
    "success"))

(defn- outer-a-worker
  "The work on one group of a in an outer join: its rows crossed with the
   rows of b's group with the same key, or followed by empty cells when b
   has none, reordered by write-index. A group is a key of mgroup-a when
   the groups are in memory, else a file under .clojask/join/a/; a b group
   file that matched is deleted, so only b's unmatched groups remain."
  [mgroup-a mgroup-b a-index b-index write-index]
  (let [a-new (range (count a-index))
        b-new (range (count b-index))
        b-nil (repeat (count b-index) nil)]
    (fn [group]
      (let [a-data (map #(u/gets % a-new) (if mgroup-a (.getKey mgroup-a group) (groupby/read-csv-seq group)))
            b-group (if mgroup-a group (string/replace-first group "/a/" "/b/"))
            b-data (when (if mgroup-a (.exists mgroup-b b-group) (.exists (io/file b-group)))
                     (let [rows (mapv #(u/gets % b-new) (if mgroup-a (.getKey mgroup-b b-group) (groupby/read-csv-seq b-group)))]
                       (when-not mgroup-a (io/delete-file b-group true))
                       rows))]
        (mapv #(u/gets % write-index)
              (if b-data
                (for [a-row a-data b-row b-data] (concat a-row b-row))
                (map #(concat % b-nil) a-data)))))))

(defn- outer-b-worker
  "The work on one group of b that matched no group of a in an outer join:
   its rows after empty cells for a, reordered by write-index."
  [mgroup-b a-index b-index write-index]
  (let [b-new (range (count b-index))
        a-nil (repeat (count a-index) nil)]
    (fn [group]
      (mapv (fn [row] (u/gets (concat a-nil (u/gets row b-new)) write-index))
            (if mgroup-b (.getKey mgroup-b group) (groupby/read-csv-seq group))))))

(defn- group-files
  "The paths of the group files in dir."
  [dir]
  (map str (rest (file-seq (io/file dir)))))

(defn start-outer
  "Append to dist every group of a
   joined to b's group with the same key, then every group of b that had
   none. The groups are in mgroup-a and mgroup-b when they are in memory,
   else in the group files under .clojask/join/."
  [num-work batch-size a b mgroup-a mgroup-b dist exception a-index b-index a-format b-format write-index out]
  (run-stage "outer join"
             #(with-open [wtr (io/writer dist :append true)]
                (let [sink (fn [rows] (out wtr rows))]
                  ;; step 1: every group of a, with its matches in b
                  (when mgroup-a
                    (doseq [key (.getKeys mgroup-a)]
                      (.delete mgroup-b key)))
                  (run num-work
                       (fn [] {:data (if mgroup-a (.getKeys mgroup-a) (group-files "./.clojask/join/a"))})
                       (outer-a-worker mgroup-a mgroup-b a-index b-index write-index)
                       sink)
                  ;; step 2: the groups of b that had no match
                  (when mgroup-b
                    (.final mgroup-b))
                  (run num-work
                       (fn [] {:data (if mgroup-b (.getKeys mgroup-b) (group-files "./.clojask/join/b"))})
                       (outer-b-worker mgroup-b a-index b-index write-index)
                       sink))))
  "success")
