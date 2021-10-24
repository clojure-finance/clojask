(ns clojask.api.gb-aggregate
  (:require [clojask.api.aggregate :refer [max min]]))
"Contains the implemented function for group-by aggregation functions"

;; (defn- compare
;;   "Specially considering nil"
;;   [a b]
;;   ())

(defn max
  [list]
  (reduce max list))

(defn min
  [list]
  (reduce min list))

