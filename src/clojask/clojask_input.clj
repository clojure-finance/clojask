(ns clojask.clojask-input
  (:require [clojure.core.async :refer [poll! timeout chan close!]]
            [clojure.set :refer [join]]
            [onyx.plugin.protocols :as p]
            [clojure.data.csv :as csv]
            [taoensso.timbre :refer [fatal info debug] :as timbre])
  (:import (java.io BufferedReader)))

(defn mem-usage
  []
  (quot (- (.totalMemory (Runtime/getRuntime)) (.freeMemory (Runtime/getRuntime))) 1048576))

(defn mem-total
  []
  (quot (.totalMemory (Runtime/getRuntime)) 1048576))

(defn mem-free
  []
  (quot (.freeMemory (Runtime/getRuntime)) 1048576))

(defn mem-max
  []
  (quot (.maxMemory (Runtime/getRuntime)) 1048576))

(defrecord AbsSeqReader [event reader rst completed? checkpoint? offset]
  p/Plugin

  (start [this event]
    this)

  (stop [this event]
    this)

  p/Checkpointed
  (checkpoint [this]
    (when checkpoint? @offset))

  (recover! [this _ checkpoint]
    (vreset! completed? false)
    (let [csv-data (rest (csv/read-csv (BufferedReader. reader)))
          data (map zipmap (repeat [:clojask-id :data]) (map vector (iterate inc 0) csv-data))]
      (if (nil? checkpoint)
        (do
          (vreset! rst data)
          (vreset! offset 0))
        (do
          (info "clojask.clojask-input is recovering state by dropping" checkpoint "elements.")
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
    (if-let [seg (first @rst)]
      (do (vswap! rst rest)
          (vswap! offset inc)
          ;; (spit "resources/debug.txt" (str seg) :append true)
          seg)
      (do (vreset! completed? true)
          nil))))

(defn input [{:keys [onyx.core/task-map] :as event}]
  ;; (println (:seq/rdr event))
  (map->AbsSeqReader {:event event
                      ;; :sequential (:seq/seq event)
                      :reader (:seq/rdr event)
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