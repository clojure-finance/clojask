(ns clojask.clojask-join
  (:require [clojask.join :as join]
            [clojure.set :as set]
            [onyx.peer.function :as function]
            [clojure.java.io :as io]
            [onyx.plugin.protocols :as p]
            [taoensso.timbre :refer [debug info] :as timbre])
  (:import (java.io BufferedReader FileReader BufferedWriter FileWriter)))

(def a (atom nil))
(def b (atom nil))
(def a-keys (atom nil))
(def b-keys (atom nil))

(defn inject-dataframe
  [d-a d-b a-key b-key]
  (reset! a d-a)
  (reset! b d-b)
  (reset! a-keys a-key)
  (reset! b-keys b-key))

(defn- inject-into-eventmap
  [event lifecycle]
  (let 
   [wtr (io/writer (:buffered-wtr/filename lifecycle) :append true)
    ;; a-map (.getKeyIndex (.col-info (deref a)))
    a-format (.getFormatter (.col-info (deref a)))
    ;; b-map (.getKeyIndex (.col-info (deref b)))
    b-format (.getFormatter (.col-info (deref b)))]
    {:clojask/wtr wtr
    ;;  :clojask/a-keys (:clojask/a-keys lifecycle)
     :clojask/a-keys (deref a-keys)
    ;;  :clojask/b-keys (:clojask/b-keys lifecycle)
     :clojask/b-keys (deref b-keys)
     :clojask/a-roll (:clojask/a-roll lifecycle)
     :clojask/b-roll (:clojask/b-roll lifecycle)
     :clojask/a-map (:clojask/a-map lifecycle)
     :clojask/b-map (:clojask/b-map lifecycle)
     :clojask/a-format a-format
     :clojask/b-format b-format
     :clojask/a-index (take (count (:clojask/a-map lifecycle)) (iterate inc 0))
     :clojask/b-index (take (count (:clojask/b-map lifecycle)) (iterate inc 0))
     :clojask/join-type (:clojask/join-type lifecycle)}))

(defn- close-writer [event lifecycle]
  (.close (:clojask/wtr event)))

;; Map of lifecycle calls that are required to use this plugin.
;; Users will generally always have to include these in their lifecycle calls
;; when submitting the job.
(def writer-join-calls
  {:lifecycle/before-task-start inject-into-eventmap
  :lifecycle/after-task-stop close-writer})

(defrecord ClojaskJoin []
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

  (write-batch [this {:keys [onyx.core/write-batch clojask/wtr clojask/a-keys clojask/b-keys clojask/a-roll clojask/b-roll  clojask/a-map clojask/b-map clojask/a-format clojask/b-format clojask/a-index clojask/b-index clojask/join-type]} replica messenger]
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
              (join/output-join wtr (:d msg) a-keys a-map b-keys (count b-map) a-roll b-roll a-format b-format a-index b-index)))

          (recur (rest batch)))))
    ;; (case join-type
    ;;   1 (loop [batch write-batch]
    ;;             (if-let [msg (first batch)]
    ;;               (do
    ;;       ;; (swap! example-datasink conj msg)
    ;;                 (if (not= (:d msg) nil)
    ;;                   (do
    ;;             ;(.write wtr (str msg "\n"))
    ;;             ;; !! define argument (debug)
    ;;         ;;   (def groupby-keys [:Department :EmployeeName])
    ;;                     (join/output-join wtr (:d msg) a-keys a-map b-keys a-format b-format a-index b-index)))

    ;;                 (recur (rest batch)))))
    ;;   2 (loop [batch write-batch]
    ;;            (if-let [msg (first batch)]
    ;;              (do
    ;;       ;; (swap! example-datasink conj msg)
    ;;                (if (not= (:d msg) nil)
    ;;                  (do
    ;;             ;(.write wtr (str msg "\n"))
    ;;             ;; !! define argument (debug)
    ;;         ;;   (def groupby-keys [:Department :EmployeeName])
    ;;                    (join/output-join-loo wtr (:d msg) a-keys a-map b-keys (count b-map) a-format b-format a-index b-index)))

    ;;                (recur (rest batch)))))
    ;;   4 (loop [batch write-batch]
    ;;               (if-let [msg (first batch)]
    ;;                 (do
    ;;       ;; (swap! example-datasink conj msg)
    ;;                   (if (not= (:d msg) nil)
    ;;                     (do
    ;;             ;(.write wtr (str msg "\n"))
    ;;             ;; !! define argument (debug)
    ;;         ;;   (def groupby-keys [:Department :EmployeeName])
    ;;                       (join/output-join-forward wtr (:d msg) a-keys a-map b-keys (count b-map) a-roll b-roll a-format b-format a-index b-index)))
    ;;                   (recur (rest batch)))))
      
    ;;   5 (loop [batch write-batch]
    ;;       (if-let [msg (first batch)]
    ;;         (do
    ;;       ;; (swap! example-datasink conj msg)
    ;;           (if (not= (:d msg) nil)
    ;;             (do
    ;;             ;(.write wtr (str msg "\n"))
    ;;             ;; !! define argument (debug)
    ;;         ;;   (def groupby-keys [:Department :EmployeeName])
    ;;               (join/output-join-backward wtr (:d msg) a-keys a-map b-keys (count b-map) a-roll b-roll a-format b-format a-index b-index)))
    ;;           (recur (rest batch))))))
    ;; (.close wtr)
    true))

;; Builder function for your output plugin.
;; Instantiates a record.
;; It is highly recommended you inject and pre-calculate frequently used data 
;; from your task-map here, in order to improve the performance of your plugin
;; Extending the function below is likely good for most use cases.
(defn join [pipeline-data]
  (->ClojaskJoin))