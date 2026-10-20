(ns clojask.classes.RowInfo)

(definterface RowIntf
  (getFilters [])
  (getAggreNewKeys [])
  (getAggreFunc [])
  (getGroupbyKeys [])
  (filter [cols predicate])
  (groupby [a])
  (aggregate [func old-key new-key])
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
