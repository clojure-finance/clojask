(ns aggregate.aggre-input
  (:require [clojure.core.async :refer [poll! timeout chan close!]]
            [clojure.set :refer [join]]
            [onyx.plugin.protocols :as p]
            [clojure.data.csv :as csv]
            [clojask.utils :refer [filter-check]]
            [taoensso.timbre :refer [fatal info debug] :as timbre]
            [clojure.java.io :as java.io])
  (:import (java.io BufferedReader)))

(defrecord AbsSeqReader [event path rst completed? checkpoint? offset]
  p/Plugin

  (start [this event]
    this)

  (stop [this event]
    this)

  p/Checkpointed
  (checkpoint [this]
    (when checkpoint? @offset))

  (recover!
   [this _ checkpoint]
   (vreset! completed? false)

   (let [directory (java.io/file path)
         files (rest (file-seq directory))
        ;;  data (map zipmap (repeat [:clojask-id :file :data]) (map vector (iterate inc 0) [files (mapv (fn [_] (read-string (str _))) files)]))
         data (do
                (def tmp (volatile! -1))
                (map (fn [file] 
                       (vswap! tmp inc)
                       {:clojask-id @tmp :file file :data (read-string (subs (str file) (inc (count (str directory)))))}) 
                     files))
         ]
     (if (nil? checkpoint)
       (do
         (vreset! rst data)
         (vreset! offset 0))
       (do
         (info "aggregate.aggre-input is recovering state by dropping" checkpoint "elements.")
         (vreset! rst (drop checkpoint data))
         (vreset! offset checkpoint)))))

  (checkpointed! [this epoch])

  p/BarrierSynchronization
  (synced? [this epoch]
    true)

  (completed? [this]
    @completed?)

  p/Input
  (poll! [this _ _]
    ;; (if (> (mem-usage) 500)
    ;;   (Thread/sleep 10))
    ;; (while (not (filter-check filters types (:data (first @rst))))
    ;;   (vswap! rst rest))
    (if-let [seg (first @rst)]
      (do
        (vswap! rst rest)
        seg)
      (do (vreset! completed? true)
          nil))
    ;; (if-let [seg (first @rst)]
    ;;   (do (vswap! rst rest)
    ;;       (vswap! offset inc)
    ;;       ;; (spit "resources/debug.txt" (str seg) :append true)
    ;;       seg)
    ;;   (do (vreset! completed? true)
    ;;       nil))
         ))

(defn inject-dataframe
  [dataframe]
  (def df dataframe))

(defn input [{:keys [onyx.core/task-map] :as event}]
  ;; (println (:seq/rdr event))
  (map->AbsSeqReader {:event event
                      ;; :sequential (:seq/seq event)
                      ;; :reader (:seq/rdr event)
                      ;; :filters (.getFilters (:row-info  df))
                      ;; :types (.getType (:col-info df))
                      :path (:buffered-reader/path event)
                      :rst (volatile! nil)
                      :completed? (volatile! false)
                      :checkpoint? (not (false? (:seq/checkpoint? task-map)))
                      :offset (volatile! nil)}))

(def reader-calls
  {})

(defn inject-lifecycle-seq
  [_ lifecycle]
  {:seq/seq (:seq/sequential lifecycle)})

(def inject-seq-via-lifecycle
  {:lifecycle/before-task-start inject-lifecycle-seq})