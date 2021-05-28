(ns clojask.ColInfo
  (:require [clojask.utils :refer :all]))

(definterface ColIntf
  (init [colNames])
  (operate [b c])
  (getMap [])
  (getKeys []))


(deftype ColInfo
  ;; the column description about whether a change is made to this column
         [^:unsynchronized-mutable col-keys
          ^:unsynchronized-mutable col-dsp]

  ;; method
   ;; method
  ColIntf
  (init
    [this colNames]
;;    (set! col-keys (map keyword colNames))
    (set! col-dsp (zipmap col-keys (map (fn [_] (str "(" (keyword _) " row)")) colNames))))
  (operate
    [this operation col]
    (if (contains? col-dsp col)
      (set! col-dsp (assoc col-dsp col (str "(" (func-name operation) " " (get col-dsp col) ")")))
      (set! col-dsp (assoc col-dsp col (str "(" (func-name operation) " row)"))) ;; would not come here otherwise deprecate
      ))
  (getMap
    [this]
    col-dsp)
  (getKeys
    [this]
    col-keys))