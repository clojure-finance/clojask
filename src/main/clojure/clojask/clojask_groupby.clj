(ns clojask.clojask-groupby
  (:require [clojask.groupby :refer [output-groupby]]
            [onyx.plugin.protocols :as p]))

(def dataframe (atom nil))
(def groupby-keys (atom nil))
(def write-index (atom nil))
(def output-func (atom nil))
(def dist (atom nil))
(def format_ (atom nil))

(defn inject-dataframe
  [df groupby-key index _dist _format]
  (reset! dataframe df)
  (reset! groupby-keys groupby-key)
  (reset! write-index index)
  (reset! dist _dist)
  (reset! format_ _format)
  )

(defn- inject-into-eventmap
  [event lifecycle]
  (let [key-index (.getKeyIndex (.col-info (deref dataframe)))
        formatters (.getFormatter (.col-info (deref dataframe)))
        groupby-keys (deref groupby-keys)]
    {:clojask/dist (deref dist)
     :clojask/groupby-keys groupby-keys
     :clojask/key-index key-index
     :clojask/formatter formatters}))

;; Map of lifecycle calls that are required to use this plugin.
;; Users will generally always have to include these in their lifecycle calls
;; when submitting the job.
(def writer-aggre-calls
  {:lifecycle/before-task-start inject-into-eventmap})

(defrecord ClojaskGroupby [write-index]
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
    (doseq [msg write-batch]
      (doseq [data (:d msg)]
        (if (not= data nil)
          (do
            (output-groupby dist data groupby-keys key-index formatter write-index (deref format_))))))
    true))

;; Builder function for your output plugin.
;; Instantiates a record.
;; It is highly recommended you inject and pre-calculate frequently used data 
;; from your task-map here, in order to improve the performance of your plugin
;; Extending the function below is likely good for most use cases.
(defn groupby [pipeline-data]
  (->ClojaskGroupby (deref write-index)))