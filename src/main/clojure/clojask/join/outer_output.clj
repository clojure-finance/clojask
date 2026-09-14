(ns clojask.join.outer-output
  (:require [onyx.plugin.protocols :as p]
            [clojure.java.io :as io]))

(def write-func (atom nil))

(defn inject-write-func
  [func]
  (reset! write-func func))

(defn- inject-into-eventmap
  [event lifecycle]
  (let [wtr (io/writer (:buffered-wtr/filename lifecycle) :append true)]
   {:clojask/wtr wtr}))

(defn- close-writer [event lifecycle]
  (.close (:clojask/wtr event)))

;; Map of lifecycle calls that are required to use this plugin.
;; Users will generally always have to include these in their lifecycle calls
;; when submitting the job.
(def writer-calls
  {:lifecycle/before-task-start inject-into-eventmap
   :lifecycle/after-task-stop close-writer})

(defrecord ClojaskOutput [write-func]
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

  (write-batch [this {:keys [onyx.core/write-batch  clojask/wtr]} replica messenger]
    (doseq [msg write-batch]
      (do
        (if (not= (:d msg) nil)
          (do
            (write-func wtr (:d msg))
            ))))
    (.flush wtr)
    true))

;; Builder function for your output plugin.
;; Instantiates a record.
;; It is highly recommended you inject and pre-calculate frequently used data 
;; from your task-map here, in order to improve the performance of your plugin
;; Extending the function below is likely good for most use cases.
(defn output [pipeline-data]
  (->ClojaskOutput (deref write-func)))