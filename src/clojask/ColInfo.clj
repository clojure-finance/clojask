(ns clojask.ColInfo
  (:require [clojask.utils :refer :all]))

(definterface ColIntf
  (init [colNames])
  (operate [b c])
  (operate [b c d])
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
    (set! col-dsp (zipmap col-keys (map vector (map vector col-keys)))))
  (operate
    [this operation col]
    (if (contains? col-dsp col)
      (do
        (set! col-dsp (assoc col-dsp col (conj (col col-dsp) operation)))
        "success")
      (str col " is not an existing column name") ;; would not come here otherwise deprecate
      ))
  (operate
   [this operation col newCol]
   (let [col (if (seq? col)
               col
               [col])
         external (vec (filter (fn [_] (not (contains? col-dsp _))) col))]
     (if (= (count external) 0)
       (if (contains? col-dsp newCol)
         (str newCol " is already exist")
         (do 
           (set! col-keys (conj col-keys newCol))
           (set! col-dsp (assoc col-dsp newCol (conj [(vec col)] operation)))))
       (do
         (str external " are not existing column names")))))
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