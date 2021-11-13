(ns clojask.clojask-aggre
  (:require [onyx.peer.function :as function]
            [onyx.plugin.protocols :as p]
            [clojure.java.io :as io]
            [taoensso.timbre :refer [debug info] :as timbre]
            [clojure.string :as string]
            [clojask.api.aggregate :refer [start]])
  (:import (java.io BufferedReader FileReader BufferedWriter FileWriter)))

(def df (atom nil))

(defn inject-dataframe
  [dataframe]
  (reset! df dataframe)
  )

(defn c-count
  [a]
  (if (coll? a)
    (count a)
    1))

(defn- inject-into-eventmap
  [event lifecycle]
  (let [wtr (io/writer (:buffered-wtr/filename lifecycle) :append true)
        order (:order lifecycle)
        aggre-func (.getAggreFunc (:row-info (deref df)))]
    {:clojask/wtr wtr
     :clojask/aggre-func aggre-func}))

(defn- close-writer [event lifecycle]
  (.close (:clojask/wtr event)))

;; Map of lifecycle calls that are required to use this plugin.
;; Users will generally always have to include these in their lifecycle calls
;; when submitting the job.
(def writer-calls
  {:lifecycle/before-task-start inject-into-eventmap
   :lifecycle/after-task-stop close-writer})

(defrecord ClojaskOutput
           [memo]
  p/Plugin
  (start [this event]
    ;; Initialize the plugin, generally by assoc'ing any initial state.
    this)

  (stop [this event]
    ;; Nothing is required here. However, most plugins have resources
    ;; (e.g. a connection) to clean up.
    ;; Mind that such cleanup is also achievable with lifecycles.
        (let [data (mapv (fn [_] (if (coll? _) _ [_])) (deref memo))]
          ;; (.write (:clojask/wtr event) (str data "\n"))
          (if (apply = (map count data))
            (mapv #(.write (:clojask/wtr event) (str (string/join "," %) "\n")) (apply map vector data))
            (throw (Exception. "aggregation result is not of the same length"))))
        this)

  p/Checkpointed
  ;; Nothing is required here. This is normally useful for checkpointing in
  ;; input plugins.
  (checkpoint [this])

  ;; Nothing is required here. This is normally useful for checkpointing in
  ;; input plugins.
  (recover! [this replica-version checkpoint])

  ;; Nothing is required here. This is normally useful for checkpointing in
  ;; input plugins.
  (checkpointed! [this epoch])

  p/BarrierSynchronization
  (synced? [this epoch]
    ;; Nothing is required here. This is commonly used to check whether all
    ;; async writes have finished.
    true)

  (completed? [this]
    ;; Nothing is required here. This is commonly used to check whether all
    ;; async writes have finished (just like synced).
    true)

  p/Output
  (prepare-batch [this event replica messenger]
    ;; Nothing is required here. This is useful for some initial preparation,
    ;; before write-batch is called repeatedly.
    true)

  (write-batch [this {:keys [onyx.core/write-batch clojask/wtr clojask/aggre-func]} replica messenger]
              ;;  keys [:Departement]
    ;; Write the batch to your datasink.
    ;; In this case we are conjoining elements onto a collection.
    (let [write-batch  write-batch]
      (doseq [msg write-batch]
      ;; (if-let [msg (first batch)]
        (do
          ;; (swap! example-datasink conj msg)
          (if (not= (:d msg) nil)
            (let [data (:d msg)]
            ;;   (.write wtr (str (string/join "," (:d msg)) "\n"))

            ;;    (swap! memo assoc index (func (get index (deref memo)) (:d msg)))
              (vreset! memo (doall (map-indexed (fn [ind prev] ((nth (nth aggre-func ind) 0) prev (nth data (nth (nth aggre-func ind) 1)))) (deref memo))))
            ;;   (.write wtr (str (vec (deref memo)) "\n"))
              )))))
    true))

;; Builder function for your output plugin.
;; Instantiates a record.
;; It is highly recommended you inject and pre-calculate frequently used data 
;; from your task-map here, in order to improve the performance of your plugin
;; Extending the function below is likely good for most use cases.
(defn output [pipeline-data]
  (let [aggre-func (.getAggreFunc (:row-info (deref df)))]
   (->ClojaskOutput (volatile! (doall (take (count aggre-func)
                                     (repeat start)))))))