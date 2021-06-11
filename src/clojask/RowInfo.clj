(ns clojask.RowInfo
  (:require [clojask.utils :refer :all]))

(definterface RowIntf
  (getFilters [])
  (filter [predicate]))

(deftype RowInfo
         [^:unsynchronized-mutable filters]
  RowIntf
  (getFilters
   [self]
   filters)
  (filter
    [self predicate]
    (set! filters (conj filters predicate))
    "success"))
