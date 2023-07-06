(ns clojask.api.gb-aggregate
  (:require [clojask.api.aggregate :as agg])
  (:refer-clojure :exclude [max min sum count]))
"Contains the implemented function for group-by aggregation functions"

;; (defn aggre-func
;;   "function that can be applied on a collection"
;;   [list])

;; single row aggregation functions

(defn max
  [list]
  (reduce agg/max list))

(defn min
  [list]
  (reduce agg/min list))

(defn sum
  [list]
  (reduce + list))

(defn count
  [list]
  (clojure.core/count list))

;; multi-row aggregation functions

(defn smallest3
  "return the smallest 3 entries"
  [list]
  (reduce agg/smallest3 agg/start list))

(defn smallestk
  "return the smallest k entries (the performance is better with smaller k)"
  [list k]
  (reduce (fn [a b] (agg/smallestk a b k)) agg/start list))

(defn largest3
  "return the largest 3 entries"
  [list]
  (reduce agg/largest3 agg/start list))

(defn largestk
  "return the largest k entries (the performance is better with smaller k)"
  [list k]
  (reduce (fn [a b] (agg/largestk a b k)) agg/start list))

