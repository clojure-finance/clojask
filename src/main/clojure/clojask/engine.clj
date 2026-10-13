(ns clojask.engine
  "A thread-pool engine for the jobs clojask runs: one input, num-work
   identical workers and one output, all in this JVM. It replaces Onyx one
   driver at a time. The system property clojask.engine, else the
   environment variable CLOJASK_ENGINE, chooses the engine: \"onyx\" (the
   default) or \"local\". Drivers not ported yet always run on Onyx."
  (:require [clojure.java.io :as io]
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

(defn- compute-worker
  "The work on one batch of a plain compute: the selected columns of every
   row that passes the filters, evaluated and formatted. Rows that fail a
   filter are dropped. Same rows as onyx-comps/worker-func-gen-format."
  [df exception index]
  (let [col-info (:col-info df)
        operations (.getDesc col-info)
        types (.getType col-info)
        formats (.getFormatter col-info)
        filters (.getFilters (:row-info df))
        index (vec index)
        eval-res (if exception u/eval-res u/eval-res-ne)]
    (fn [batch]
      (into []
            (keep (fn [row]
                    (when (u/filter-check filters types row)
                      (mapv (fn [i] ((or (get formats i) str) (eval-res row types formats operations i))) index))))
            batch))))

(defn start-compute
  "The local engine's start-onyx: compute df into dist, appending after
   the header that DataFrame.compute may have written. Output is always in
   input order."
  [num-work batch-size df dist exception index melt out]
  (let [work (try
               (compute-worker df exception index)
               (catch Throwable e
                 (throw (stage-error "preparing stage (compute)" e))))]
    (try
      (with-open [wtr (io/writer dist :append true)]
        (run num-work
             (fn []
               (let [{:keys [data close]} ((.getFunc df) :map)]
                 {:data (partition-all batch-size data) :close close}))
             work
             (fn [rows]
               (doseq [row rows]
                 (out wtr (melt row))))))
      (catch Throwable e
        (throw (stage-error "run stage (compute)" e))))
    "success"))
