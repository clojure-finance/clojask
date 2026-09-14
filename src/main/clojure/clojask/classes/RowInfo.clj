(ns clojask.classes.RowInfo)

(definterface RowIntf
  (getFilters [])
  (getAggreNewKeys [])
  (getAggreFunc [])
  (getGroupbyKeys [])
  (filter [cols predicate])
  (groupby [a])
  (aggregate [func old-key new-key])
  (setRowInfo [new-col-desc new-col-set])
  (copy [])
  (rollback [])
  (commit []))

(deftype RowInfo
         [^:unsynchronized-mutable filters
          ^:unsynchronized-mutable groupby-key
          ^:unsynchronized-mutable aggre-func
          ^:unsynchronized-mutable aggre-new-key
          ^:unsynchronized-mutable hist]
  RowIntf
  (getFilters
    [this]
    filters)

  (getGroupbyKeys
    [this]
    groupby-key)

  (getAggreNewKeys
    [this]
    aggre-new-key)

  (getAggreFunc
    [this]
    aggre-func)

  (filter
    [this cols predicate]
    (.copy this)
    (set! filters (conj filters [predicate cols]))
    nil)

  (groupby
    [this key]
    (.copy this)
    (set! groupby-key key)
    nil)

  (aggregate
    [this func old-keys new-keys]
    (.copy this)
    (doseq [old-key old-keys]
      (set! aggre-func (conj aggre-func [func old-key])))
    (doseq [new-key new-keys]
      (set! aggre-new-key (conj aggre-new-key new-key)))
    nil)

  (setRowInfo
    [this new-col-desc new-col-set]
    (.copy this)
    (let [original-filter (.getFilters this)
          original-groupby-keys (.getGroupbyKeys this)
          original-aggre-func (.getAggreFunc this)
          new-filter-fns (map #(first %) original-filter)
          new-filter-cols (map (fn [fcols] (map #(first (first (get new-col-desc %))) fcols)) (doall (map #(last %) original-filter)))
          new-groupby-fns (map #(first %) original-groupby-keys)
          new-groupby-cols (map #(first (first (get new-col-desc (last %)))) original-groupby-keys)
          new-aggre-fns (map #(first %) original-aggre-func)
          new-aggre-cols (map #(first (first (get new-col-desc (last %)))) original-aggre-func)]
      (if (not (empty? (.getFilters this)))
        (set! filters (vec (map vector new-filter-fns new-filter-cols))))
      (if (not (empty? (.getGroupbyKeys this)))
        (set! groupby-key (vec (map vector new-groupby-fns new-groupby-cols)))
        )
      (if (not (empty? (.getAggreFunc this)))
        (set! aggre-func (vec (map vector new-aggre-fns new-aggre-cols))))))

  (copy
    [this]
    (set! hist {:filters filters
                :groupby-key groupby-key
                :aggre-func aggre-func
                :aggre-new-key aggre-new-key}))

  (rollback
    [this]
    (if (not= hist {})
      (do (set! filters (:filters hist))
          (set! groupby-key (:groupby-key hist))
          (set! aggre-func (:aggre-func hist))
          (set! aggre-new-key (:aggre-new-key hist)))))
  
  (commit
   [this]
   (set! hist {})))
