(ns clojask.clojask-output
  (:require [onyx.peer.function :as function]
            [onyx.plugin.protocols :as p]
            [clojure.java.io :as io]
            [taoensso.timbre :refer [debug info] :as timbre]
            [clojure.string :as string]
            [clojure-heap.core :as heap]
            [clojure.set :as set]
            [clojask.join.outer-output :as output])
  (:import (java.io BufferedReader FileReader BufferedWriter FileWriter)))

(def df (atom nil))
(def output-func (atom nil))

(defn inject-dataframe
  [dataframe out]
  (reset! df dataframe)
  (reset! output-func out)
  )

(defn- inject-into-eventmap
  [event lifecycle]
  (let [wtr (io/writer (:buffered-wtr/filename lifecycle) :append true)
        order (:order lifecycle)
        indices (:indices lifecycle)
        formatter (.getFormatter (:col-info (deref df)))]
   {:clojask/wtr wtr :clojask/order order 
    ;; :clojask/formatter (set/rename-keys formatter (zipmap indices (iterate inc 0)))
    }))

(defn- close-writer [event lifecycle]
  (.close (:clojask/wtr event)))

(defn- write-msg
  [wtr msg melt output-func]
  (if (not= (:d msg) nil)
    (doseq []
      (output-func wtr (melt (:d msg)))
                ;; !! define argument (debug)
      )))

(defn- order-write
  [wtr msg heap exp-id melt output-func]
  (let [id (:id msg)]
    ;; (println (str msg " " (deref exp-id)))
    (if (= id (deref exp-id))
      (do
        (write-msg wtr msg melt output-func)
        (swap! exp-id inc)
        (while (= (:id (heap/peek heap)) (deref exp-id))
          (write-msg wtr (heap/poll heap) melt output-func)
          (swap! exp-id inc)))
      (do
        (heap/add heap msg)
        ;; (println (heap/get-size heap))
        )
      )))

;; Map of lifecycle calls that are required to use this plugin.
;; Users will generally always have to include these in their lifecycle calls
;; when submitting the job.
(def writer-calls
  {:lifecycle/before-task-start inject-into-eventmap
   :lifecycle/after-task-stop close-writer})

(def melt (atom nil))

(defn inject-melt
  [tmp]
  (reset! melt tmp))

(defrecord ClojaskOutput [melt heap exp-id output]
  p/Plugin
  (start [this event]
    ;; Initialize the plugin, generally by assoc'ing any initial state.
    this)

  (stop [this event]
    ;; Nothing is required here. However, most plugins have resources
    ;; (e.g. a connection) to clean up.
    ;; Mind that such cleanup is also achievable with lifecycles.
        ;; (println (heap/get-size heap))
        (if (not= (heap/get-size heap) 0) (throw (Exception. (str "The order enforcement failed. "  (heap/get-size heap) " rows have been shuffled or missing."))))
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

  (write-batch [this {:keys [onyx.core/write-batch  clojask/wtr clojask/order]} replica messenger]
              ;;  keys [:Departement]
    ;; Write the batch to your datasink.
    ;; In this case we are conjoining elements onto a collection.
    (if order
      (doseq [msg write-batch]
        (order-write wtr msg heap exp-id melt output))
      (let []
        (doseq [msg write-batch]
          ;; (println msg)
          (write-msg wtr msg melt output))))
    true))

;; Builder function for your output plugin.
;; Instantiates a record.
;; It is highly recommended you inject and pre-calculate frequently used data 
;; from your task-map here, in order to improve the performance of your plugin
;; Extending the function below is likely good for most use cases.
(defn output [pipeline-data]
  (->ClojaskOutput (deref melt) (heap/heap (fn [a b] (<= (:id a) (:id b)))) (atom 0) (deref output-func)))