(ns clojask.join.outer-input
  (:require [onyx.plugin.protocols :as p]
            [clojask.utils]
            [taoensso.timbre :refer [info]]
            [clojure.java.io :as java.io]))

(def mgroup-a nil)
(def mgroup-b nil)

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
         files (if (= mgroup-a nil)
                 (rest (file-seq directory))
                 (.getKeys mgroup-a))
         data 
         (if (= mgroup-a nil)
                (do
                  (def tmp (volatile! -1))
                  (map (fn [file]
                         (vswap! tmp inc)
                         {:id @tmp :d (str file)})
                       files))
                (do
                  (def tmp (volatile! -1))
                  (map (fn [file]
                         (vswap! tmp inc)
                         (if (not= nil mgroup-b) (.delete mgroup-b file))
                         {:id @tmp :d (str file)})
                       files)))
     ]
     (if (nil? checkpoint)
       (do
         (vreset! rst data)
         (vreset! offset 0))
       (do
         (info "clojask.join.outer-input is recovering state by dropping" checkpoint "elements.")
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
        seg)
      (do (vreset! completed? true)
          nil))
         ))

(defn inject-dataframe
  [_mgroup-a _mgroup-b]
  (def mgroup-a _mgroup-a)
  (def mgroup-b _mgroup-b))

(defn input [{:keys [onyx.core/task-map] :as event}]
  (map->AbsSeqReader {:event event
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