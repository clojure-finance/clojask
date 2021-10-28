(ns clojask.api.gb-aggregate
  (:require [clojask.api.aggregate :as aggre]))
"Contains the implemented function for group-by aggregation functions"

;; (defn- compare
;;   "Specially considering nil"
;;   [a b]
;;   ())

(defn max
  [list]
  (reduce aggre/max list))

(defn min
  [list]
  (reduce aggre/min list))

