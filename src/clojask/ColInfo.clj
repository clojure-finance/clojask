(ns clojask.ColInfo
  (:require [clojask.utils :refer []]))

(definterface ColIntf
  (init [colNames])
  (operate [b c])
  (operate [b c d])
  (setType [b c])
  (getDesc [])
  (getType [])
  (getKeys [])
  (getKeyIndex [])
  (getIndexKey [])
  (setFormatter [b c])
  (getFormatter [])
  (renameCol [newColNames]))


(deftype ColInfo
  ;; the column description about whether a change is made to this column
         [^:unsynchronized-mutable col-keys
          ^:unsynchronized-mutable key-index
          ^:unsynchronized-mutable index-key
          ^:unsynchronized-mutable col-dsp
          ^:unsynchronized-mutable col-type
          ^:unsynchronized-mutable col-format]

  ;; method
   ;; method
  ColIntf
  ;; (init
  ;;   [this colNames]
  ;;   (set! col-keys (map keyword colNames))
  ;;   (set! col-dsp (zipmap col-keys (map (fn [_] (wrap-res _ row)) col-keys))))
  (init
    [this colNames]
    (set! col-keys (vec colNames))  ;; contains only the original keys
    (set! key-index (zipmap col-keys (iterate inc 0)))
    (set! index-key (zipmap (iterate inc 0) col-keys))
    (set! col-dsp (zipmap (take (count colNames) (iterate inc 0)) (map vector (map vector (iterate inc 0))))))
  (operate
    [this operation col]
    (if (contains? key-index col)
      (do
        (set! col-dsp (assoc col-dsp (get key-index col) (conj (get col-dsp (get key-index col)) operation)))
        "success")
      (str col " is not an existing column name") ;; would not come here otherwise deprecate
      ))
  (operate
    [this operation col newCol]
    (let [col (if (coll? col)
                col
                [col])
          external (vec (filter (fn [_] (not (.contains col-keys _))) col))]
      (if (= (count external) 0)
        (if (contains? key-index newCol)
          (str newCol " is already exist")
          (do
            ;; (set! col-keys (conj col-keys newCol))
            (set! key-index (assoc key-index newCol (count key-index)))
            (set! index-key (assoc index-key (count index-key) newCol))
            (set! col-dsp (assoc col-dsp (get key-index newCol) (conj [(vec (map (fn [_] (get key-index _)) col))] operation)))))
        (do
          (str external " are not original column names")))))
  (setType
    [this operation col]
    (if (.contains col-keys col)
      ;; if this column has been assigned a type
      (do
        (set! col-type (assoc col-type (get key-index col)  operation))
          ;; (set! col-dsp (assoc col-dsp col (vec (concat (conj [(first (col col-dsp))] operation) (rest (rest (col col-dsp)))))))
        "success")
      "There is no such column name."))
  (setFormatter
   [this format col]
   (set! col-format (assoc col-format (get key-index col) format)))
  (getFormatter
   [this]
   col-format)
  (getDesc
    [this]
    col-dsp)
  (getType
    [this]
    col-type)
  (getKeys
    [this]
    col-keys)
  (getKeyIndex
   [this]
   key-index)
  (getIndexKey
   [this]
   index-key)
  (renameCol
    [this newColNames]
    (set! col-keys (vec newColNames))))