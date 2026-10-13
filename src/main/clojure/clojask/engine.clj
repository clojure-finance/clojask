(ns clojask.engine
  "A thread-pool engine for the jobs clojask runs: one input, num-work
   identical workers and one output, all in this JVM. It replaces Onyx one
   driver at a time. The system property clojask.engine, else the
   environment variable CLOJASK_ENGINE, chooses the engine: \"onyx\" (the
   default) or \"local\". Drivers not ported yet always run on Onyx."
  (:require [clojure.java.io :as io]
            [clojask.api.aggregate :refer [start]]
            [clojask.groupby :as groupby]
            [clojask.utils :as u])
  (:import [com.clojask.exception ExecutionException]
           [java.util.concurrent ArrayBlockingQueue Callable ExecutorService
            Executors Future ThreadFactory TimeUnit]))

(defn engine
  "The engine for the next compute, :onyx or :local."
  []
  (let [v (or (System/getProperty "clojask.engine") (System/getenv "CLOJASK_ENGINE") "onyx")]
    (case v
      "onyx" :onyx
      "local" :local
      (throw (IllegalArgumentException. (str "Unknown clojask engine \"" v "\"; use \"onyx\" or \"local\"."))))))

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
   that fail a filter are dropped. Same rows as onyx-comps/worker-func-gen
   and worker-func-gen-format."
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
  "The local engine's start-onyx: compute df into dist, appending after
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
  "The local engine's start-onyx-aggre-only: fold the rows that pass the
   filters through aggre-func, pairs of a function and a position in
   index, then append the result rows to dist. Rows are folded in input
   order. Same output as clojask-aggre."
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
  "The local engine's start-onyx-groupby: add every row that passes the
   filters to its group, a file under dist when dist is a directory path,
   otherwise the in-memory MGroup dist. Same groups as clojask-groupby."
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
   groups are in memory. Same rows as aggre-onyx-comps/worker-func-gen."
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
  "The local engine's start-onyx-aggre: aggregate every group on the pool
   and append the result rows to dist, in the order the groups are listed.
   Same output as aggre-output."
  [num-work batch-size df source dist exception aggre-func index formatter out]
  (let [work (prepare "groupby aggregate" #(group-worker df aggre-func index formatter source))]
    (run-stage "groupby aggregate"
               #(with-open [wtr (io/writer dist :append true)]
                  (run num-work (groups source) work
                       (fn [rows]
                         (out wtr rows)))))
    "success"))
