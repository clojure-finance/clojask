(ns clojask.ColInfo
  (:require [clojask.utils :refer :all]))

(definterface ColIntf
  (init [colNames])
  (operate [b c])
  (setType [b c])
  (getDesc [])
  (getType [])
  (getKeys []))


(deftype ColInfo
  ;; the column description about whether a change is made to this column
         [^:unsynchronized-mutable col-keys
          ^:unsynchronized-mutable col-dsp
          ^:unsynchronized-mutable col-type]

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
  (setType
    [this operation col]
    (if (contains? col-dsp col)
      (if (contains? col-type col)   ;; if this column has been assigned a type
        (do
          (set! col-type (assoc col-type col (get operation-type-map operation)))
          (set! col-dsp (assoc col-dsp col (vec (concat (conj [(first (col col-dsp))] operation) (rest (rest (col col-dsp)))))))
          "success")
        (do
          (set! col-type (assoc col-type col (get operation-type-map operation)))
          (set! col-dsp (assoc col-dsp col (vec (concat (conj [(first (col col-dsp))] operation) (rest (col col-dsp))))))
          "success"))
      "There is no such column name."))
  (getDesc
    [this]
    col-dsp)
  (getType
    [this]
    col-type)
  (getKeys
    [this]
    col-keys))