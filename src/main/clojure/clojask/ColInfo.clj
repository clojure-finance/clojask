(ns clojask.ColInfo
  (:require [clojure.set :as set]
            [clojask.utils :refer []]))

(import '[com.clojask.exception Clojask_TypeException]
        '[com.clojask.exception Clojask_OperationException])

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
  (getDeletedCol [] "get indices of deleted columns")
  (setFormatter [b c])
  (getFormatter [])
  (delCol [col-to-del])
  (setColInfo [new-col-set])
  (renameColInfo [new-col-names]))


(deftype ColInfo
  ;; the column description about whether a change is made to this column
         [^:unsynchronized-mutable col-keys
          ^:unsynchronized-mutable key-index
          ^:unsynchronized-mutable index-key
          ^:unsynchronized-mutable col-dsp
          ^:unsynchronized-mutable col-type
          ^:unsynchronized-mutable col-format
          ^:unsynchronized-mutable col-deleted]

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
    (set! col-dsp (zipmap (take (count colNames) (iterate inc 0)) (map vector (map vector (iterate inc 0)))))
    (set! col-deleted (set nil)))

  (operate
    [this operation col]
    (if (contains? key-index col)
      (do
        (set! col-dsp (assoc col-dsp (get key-index col) (conj (get col-dsp (get key-index col)) operation)))
        ; "success"
        nil)
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
            (set! col-dsp (assoc col-dsp (get key-index newCol) (conj [(vec (map (fn [_] (get key-index _)) col))] operation)))
            ; "success"
            nil))
        (do
          (throw (Clojask_OperationException. (str external " are not original column names")))
          ))))

  (setType
    [this operation col]
    (if (.contains col-keys col)
      ;; if this column has been assigned a type
      (do
        (set! col-type (assoc col-type (get key-index col)  operation))
          ;; (set! col-dsp (assoc col-dsp col (vec (concat (conj [(first (col col-dsp))] operation) (rest (rest (col col-dsp)))))))
        ; "success"
        nil)
      (throw (Clojask_OperationException. "Column name passed to setType not found"))))

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

  (getDeletedCol
    [this]
    col-deleted)

  (delCol
    [this col-to-delete]
    (let [col-indices (set (map key-index col-to-delete))]
      (set! col-deleted (set/union col-deleted col-indices))))

  (setColInfo
    [this new-col-set]
    (let [original-key-index (.getKeyIndex this)
          new-col-dsp-vals (vals (select-keys original-key-index new-col-set))
          original-type (.getType this)
          original-format (.getFormatter this)]
      (set! col-keys (vec new-col-set))
      (set! key-index (zipmap new-col-set (iterate inc 0)))
      (set! index-key (zipmap (iterate inc 0) new-col-set))
      (set! col-dsp (zipmap (take (count col-keys) (iterate inc 0)) (map vector (map vector new-col-dsp-vals))))
      (if (not (empty? (.getType this)))
          (set! col-type (zipmap (map #(first (first (get col-dsp (first %)))) original-type) (map last original-type))))
      (if (not (empty? (.getFormatter this)))
          (set! col-format (zipmap (map #(first (first (get col-dsp (first %)))) original-format) (map last original-format))))
    ))
  
  (renameColInfo
    [this new-col-names]
    (set! col-keys (vec new-col-names))
    (set! key-index (zipmap new-col-names (map #(last %) (.getKeyIndex this))))
    (set! index-key (zipmap (map #(first %) (.getIndexKey this)) new-col-names))
    ))