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
  (reorderCol [new-col-desc new-col-order]))

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
   "success")
  (groupby
    [self key]
    (set! groupby-key key)
    "success")
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
        "success")
      "failed: you must first group the dataframe by some keys then aggregate"))
  (reorderCol
    [self new-col-desc new-col-order]
    (println new-col-desc)
    (println "Before re-ordering...")
    (println (.getFilters self))
    (println (.getGroupbyKeys self))
    (println (.getAggreFunc self))
    (println (.getAggreNewKeys self))
    (println "After re-ordering...")
    (let [original-filter (.getFilters self)
          original-groupby-keys (.getGroupbyKeys self)
          new-filter-fns (map #(first %) original-filter)
          new-filter-cols (map (fn [fcols] (map #(first (first (get new-col-desc %))) fcols)) (doall (map #(last %) original-filter)))]
      (if (not (empty? (.getFilters self)))
        (set! filters (vec (map vector new-filter-fns new-filter-cols))))
      (if (not (empty? (.getGroupbyKeys self)))
        (set! groupby-key (vec (map #(first (first (get new-col-desc %))) original-groupby-keys))))
      )
      (println (.getFilters self))
      (println (.getGroupbyKeys self))
      )
  )
