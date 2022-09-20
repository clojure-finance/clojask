(ns clojask.classes.ColInfo
  (:require [clojure.set :as set]
            [clojask.utils :refer []]))

(import '[com.clojask.exception TypeException]
        '[com.clojask.exception OperationException])

(definterface ColIntf
  (init [colNames])
  (operate [operation col])
  (operate [operation col newCol])
  (setType [operation col])
  (getDesc [] "get column description")
  (getType [] "get column type")
  (getKeys [] "get collection of keys")
  (getKeyIndex [] "get map with key = column name, value = index")
  (getIndexKey [] "get map with key = index, value = column name")
  (getDeletedCol [] "get indices of deleted columns")
  (setFormatter [format col])
  (getFormatter [])
  (delCol [col-to-del])
  (setColInfo [new-col-set])
  (renameColInfo [old-col new-col])
  (copy [] "copy all the information for rollback purpose")
  (rollback [] "undo the change making use of the copied")
  (commit [])
  )


(deftype ColInfo
  ;; the column description about whether a change is made to this column
         [^:unsynchronized-mutable col-keys
          ^:unsynchronized-mutable key-index
          ^:unsynchronized-mutable index-key
          ^:unsynchronized-mutable col-dsp
          ^:unsynchronized-mutable col-type
          ^:unsynchronized-mutable col-format
          ^:unsynchronized-mutable col-deleted
          ^:unsynchronized-mutable hist]

  ;; method
  ColIntf

  (init
    [this colNames]
    (set! col-keys (vec colNames))  ;; contains only the original keys
    (set! key-index (zipmap col-keys (iterate inc 0)))
    (set! index-key (zipmap (iterate inc 0) col-keys))
    (set! col-dsp (zipmap (take (count colNames) (iterate inc 0)) (map vector (map vector (iterate inc 0)))))
    (set! col-deleted (set nil)))

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
    (mapv (fn [index] (get index-key index))
          (take (count index-key) (iterate inc 0))))

  (getKeyIndex
    [this]
    key-index)

  (getIndexKey
    [this]
    index-key)

  (getDeletedCol
    [this]
    col-deleted)

  (operate
    [this operation col]
    (.copy this)
    (if (contains? key-index col)
      (do
        (set! col-dsp (assoc col-dsp (get key-index col) (conj (get col-dsp (get key-index col)) operation)))
          ;; "success"
        nil)
      (throw (OperationException. "Column name passed to operate not found"))))

  (operate
    [this operation col newCol]
    (.copy this)
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
            ;; "success"
            nil))
        (do
          (throw (OperationException. (str external " are not original column names")))))))

  (setType
    [this operation col]
    (.copy this)
    (if (.contains col-keys col)
      ;; if this column has been assigned a type
      (do
        (set! col-type (assoc col-type (get key-index col)  operation))
        ;; (set! col-dsp (assoc col-dsp col (vec (concat (conj [(first (col col-dsp))] operation) (rest (rest (col col-dsp)))))))
        ;; "success"
        nil)
      (throw (OperationException. "Column name passed to setType not found"))))

  (setFormatter
    [this format col]
    (.copy this)
    (set! col-format (assoc col-format (get key-index col) format)))

  (delCol
    [this col-to-delete]
    (.copy this)
    (let [col-indices (set (map key-index col-to-delete))]
      (set! col-deleted (set/union col-deleted col-indices))))

  (setColInfo
    [this new-col-set]
    (.copy this)
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
        (set! col-format (zipmap (map #(first (first (get col-dsp (first %)))) original-format) (map last original-format))))))

  (renameColInfo
    [this old-col new-col]
    (.copy this)
    (set! col-keys (mapv (fn [_] (if (= _ old-col) new-col _)) col-keys))
    (let [index (get key-index old-col)]
      (set! key-index (set/rename-keys key-index {old-col new-col}))
      (set! index-key (update index-key index (fn [_] new-col)))))

  (copy
    [this]
    (set! hist {:col-keys col-keys
                :key-index key-index
                :index-key index-key
                :col-dsp col-dsp
                :col-type col-type
                :col-format col-format
                :col-deleted col-deleted}))

  (rollback
   [this]
   (if (not= hist {})
     (do (set! col-keys (:col-keys hist))  ;; contains only the original keys
         (set! key-index (:key-index hist))
         (set! index-key (:index-key hist))
         (set! col-type (:col-type hist))
         (set! col-format (:col-format hist))
         (set! col-dsp (:col-dsp hist))
         (set! col-deleted (:col-deleted hist)))))
  
  (commit
   [this]
   (set! hist {})))