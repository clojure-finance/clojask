(ns clojask.DataStat
  (:require [clojure.java.io :as io]))

(import '[com.clojask.exception TypeException]
        '[com.clojask.exception OperationException])

(definterface DataIntf
  (init [source])
  (getSize []))


(deftype DataStat
  ;; the column description about whether a change is made to this column
         [^:unsynchronized-mutable file-size
          ^:unsynchronized-mutable num-rows]

  ;; method
  DataIntf

  (init
    [this source]
    (if (fn? source)
      (do
        (set! file-size nil)
        (set! num-rows nil))
      (do
        (set! file-size (.length (io/file source))))))

  (getSize
    [this]
    file-size))

(defn compute-stat
  [source]
  (let [stat (DataStat. nil nil)]
    (.init stat source)
    stat))