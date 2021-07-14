(ns clojask.RowInfo
  (:require [clojask.utils :refer :all]))

(definterface RowIntf
  (getFilters [])
  (getAggreOldKeys [])
  (getAggreNewKeys [])
  (getAggreFunc [])
  (getGroupbyKeys [])
  (filter [cols predicate])
  (groupby [a])
  (aggregate [func old-key new-key]))

(deftype RowInfo
         [^:unsynchronized-mutable filters
          ^:unsynchronized-mutable groupby-key
          ^:unsynchronized-mutable aggre-func
          ^:unsynchronized-mutable aggre-old-key
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
  (getAggreOldKeys
   [self]
   aggre-old-key)
  (getAggreNewKeys
   [self]
   aggre-new-key)
  (getAggreFunc
   [self]
   aggre-func)
  (aggregate
    [self func old-key new-key]
    (if (not= groupby-key [])
      (do
        (set! aggre-func func)
        (set! aggre-old-key old-key)
        (set! aggre-new-key new-key)
        "success")
      "failed: you must first group the dataframe by some keys then aggregate")))
