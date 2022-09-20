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
  (count list))

;; multi-row aggregation functions

(defn top3
  [list]
  (reduce agg/top3 agg/start list))

(defn bottom3
  [list]
  (reduce agg/bottom3 agg/start list))

