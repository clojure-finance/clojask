(ns clojask.RowInfo
  (:require [clojask.utils :refer :all]))

(definterface RowIntf
  (getFilters [])
  (getAggreKey [])
  (getGroupbyKeys [])
  (filter [predicate])
  (groupby [a])
  (aggregate [func new-key]))

(deftype RowInfo
         [^:unsynchronized-mutable filters
          ^:unsynchronized-mutable groupby-key
          ^:unsynchronized-mutable aggre-func
          ^:unsynchronized-mutable aggre-key]
  RowIntf
  (getFilters
    [self]
    filters)
  (filter
    [self predicate]
    (set! filters (conj filters predicate))
    "success")
  (groupby
    [self key]
    (set! groupby-key key)
    "success")
  (getGroupbyKeys
   [self]
   groupby-key)
  (getAggreKey
   [self]
   aggre-key)
  (aggregate
    [self func new-key]
    (if (not= groupby-key [])
      (do
        (set! aggre-func func)
        (set! aggre-key new-key)
        "success")
      "failed: you must first group the dataframe by some keys then aggregate")))
