(ns clojask.clojask-groupby
  (:require [clojask.groupby :refer [output-groupby]]
            [onyx.peer.function :as function]
            [onyx.plugin.protocols :as p]
            [clojure.set :as set]
            [taoensso.timbre :refer [debug info] :as timbre])
  (:import (java.io BufferedReader FileReader BufferedWriter FileWriter)))

(def dataframe (atom nil))

(defn inject-dataframe
  [df]
  (reset! dataframe df))

(defn- inject-into-eventmap
  [event lifecycle]
  (let [key-index (.getKeyIndex (.col-info (deref dataframe)))
        formatters (.getFormatter (.col-info (deref dataframe)))
        groupby-keys (.getGroupbyKeys (.row-info (deref dataframe)))]
  ;;  [wtr (BufferedWriter. (FileWriter. (:buffered-wtr/filename lifecycle)))]
    {:clojask/dist (:buffered-wtr/filename lifecycle) 
    ;;  :clojask/groupby-keys (:clojask/groupby-keys lifecycle) 
     :clojask/groupby-keys groupby-keys
     :clojask/key-index key-index
     :clojask/formatter formatters}))

(defn- close-writer [event lifecycle]
  (.close (:clojask/wtr event)))

;; Map of lifecycle calls that are required to use this plugin.
;; Users will generally always have to include these in their lifecycle calls
;; when submitting the job.
(def writer-aggre-calls
  {:lifecycle/before-task-start inject-into-eventmap})

(defrecord ClojaskGroupby []
  p/Plugin
  (start [this event]
    ;; Initialize the plugin, generally by assoc'ing any initial state.
    this)

  (stop [this event]
    ;; Nothing is required here. However, most plugins have resources
    ;; (e.g. a connection) to clean up.
    ;; Mind that such cleanup is also achievable with lifecycles.
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

  (write-batch [this {:keys [onyx.core/write-batch clojask/dist clojask/groupby-keys clojask/key-index clojask/formatter]} replica messenger]
              ;;  keys [:Departement]
    ;; Write the batch to your datasink.
    ;; In this case we are conjoining elements onto a collection.
    (loop [batch write-batch]
      (if-let [msg (first batch)]
        (do
          ;; (swap! example-datasink conj msg)
          (if (not= (:d msg) nil)
            (do
                ;(.write wtr (str msg "\n"))
                ;; !! define argument (debug)
            ;;   (def groupby-keys [:Department :EmployeeName])
              (output-groupby dist (:d msg) groupby-keys key-index formatter)))

          (recur (rest batch)))))
    true))

;; Builder function for your output plugin.
;; Instantiates a record.
;; It is highly recommended you inject and pre-calculate frequently used data 
;; from your task-map here, in order to improve the performance of your plugin
;; Extending the function below is likely good for most use cases.
(defn groupby [pipeline-data]
  (->ClojaskGroupby))