(ns clojask.RowInfo
  ;; (:require [clojask.utils :refer :all])
  )

(definterface RowIntf
  (getFilters [])
  (getAggreOldKeys [])
  (getAggreNewKeys [])
  (getAggreFunc [])
  (getGroupbyKeys [])
  (filter [cols predicate])
  (groupby [a])
  (aggregate [func old-key new-key])
  (setRowInfo [new-col-desc new-col-set])
  (renameRowInfo [new-col-names]))

(deftype RowInfo
         [^:unsynchronized-mutable filters
          ^:unsynchronized-mutable groupby-key
          ^:unsynchronized-mutable aggre-func
          ;; ^:unsynchronized-mutable aggre-old-key
          ^:unsynchronized-mutable aggre-new-key]
  RowIntf
  (getFilters
    [self]
    filters)
  (filter
   [self cols predicate]
   (set! filters (conj filters [predicate cols]))
   ; "success"
   nil)
  (groupby
    [self key]
    (set! groupby-key key)
    ; "success"
    nil)
  (getGroupbyKeys
   [self]
   groupby-key)
  ;; (getAggreOldKeys
  ;;  [self]
  ;;  aggre-old-key)
  (getAggreNewKeys
   [self]
   aggre-new-key)
  (getAggreFunc
   [self]
   aggre-func)
  (aggregate
    [self func old-keys new-keys]
    (if (not= groupby-key [])
      (do
        (doseq [old-key old-keys]
          (set! aggre-func (conj aggre-func [func old-key])))
        ;; (set! aggre-old-key old-key)
        (doseq [new-key new-keys]
         (set! aggre-new-key (conj aggre-new-key new-key)))
        ; "success"
        nil)
      "failed: you must first group the dataframe by some keys then aggregate"))
  (setRowInfo
    [self new-col-desc new-col-set]
    (let [original-filter (.getFilters self)
          original-groupby-keys (.getGroupbyKeys self)
          original-aggre-func (.getAggreFunc self)
          new-filter-fns (map #(first %) original-filter)
          new-filter-cols (map (fn [fcols] (map #(first (first (get new-col-desc %))) fcols)) (doall (map #(last %) original-filter)))
          new-aggre-fns (map #(first %) original-aggre-func)
          new-aggre-cols (map #(first (first (get new-col-desc (last %)))) original-aggre-func)]
      (if (not (empty? (.getFilters self)))
        (set! filters (vec (map vector new-filter-fns new-filter-cols))))
      (if (not (empty? (.getGroupbyKeys self)))
        (set! groupby-key (vec (map #(first (first (get new-col-desc %))) original-groupby-keys))))
      (if (not (empty? (.getAggreFunc self)))
        (set! aggre-func (vec (map vector new-aggre-fns new-aggre-cols))))
      ))
  )
