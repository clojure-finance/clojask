(ns clojask.clojask-input
  (:require [onyx.plugin.protocols :as p]
            [clojask.utils]
            [taoensso.timbre :refer [info]]))

(defrecord AbsSeqReader [event reader filters types have-col rst completed? checkpoint? offset batch-size]
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
    (let [csv-data (reader)
          data (map zipmap (repeat [:id :d]) (map vector (iterate inc 0) (partition batch-size batch-size [] csv-data)))]
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
    (if-let [seg (first @rst)]
      (do
        (vswap! rst rest)
        seg
        )
      (do (vreset! completed? true)
          nil))
         ))

(defn inject-dataframe
  [dataframe]
  (def df dataframe))

(defn input [{:keys [onyx.core/task-map] :as event}]
  (map->AbsSeqReader {:event event
                      :reader (.getFunc df)
                      :filters (.getFilters (:row-info df))
                      :types (.getType (:col-info df))
                      :have-col (:have-col df)
                      :rst (volatile! nil)
                      :completed? (volatile! false)
                      :checkpoint? (not (false? (:seq/checkpoint? task-map)))
                      :offset (volatile! nil)
                      :batch-size (:batch-size df)}))

(def reader-calls
  {})

(defn inject-lifecycle-seq
  [_ lifecycle]
  {:seq/seq (:seq/sequential lifecycle)})

(def inject-seq-via-lifecycle
  {:lifecycle/before-task-start inject-lifecycle-seq})