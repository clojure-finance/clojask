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
  ;; (init
  ;;   [this colNames]
  ;;   (set! col-keys (map keyword colNames))
  ;;   (set! col-dsp (zipmap col-keys (map (fn [_] (wrap-res _ row)) col-keys))))
  (init
    [this colNames]
    (set! col-keys (map keyword colNames))
    (set! col-dsp (zipmap col-keys (map vector col-keys))))
  (operate
    [this operation col]
    (if (contains? col-dsp col)
      (set! col-dsp (assoc col-dsp col (conj (col col-dsp) operation)))
      (set! col-dsp (assoc col-dsp col (conj [col] operation))) ;; would not come here otherwise deprecate
      ))
  (getMap
    [this]
    col-dsp)
  (getKeys
    [this]
    col-keys))