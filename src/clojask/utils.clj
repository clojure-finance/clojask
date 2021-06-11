(ns clojask.utils
  (:require [clojure.core.async :refer [chan sliding-buffer >!! close!]]
            [clojure.java.io :refer [resource]]
            [onyx.plugin.core-async :refer [take-segments!]])
  (:import (java.util Date)))
"Utility function used in dataframe"


(defn eval-res
  [row opr-vec]
  ;; (println row)
  ;; (println opr-vec)
  (let [vals (vals (select-keys row (first opr-vec)))]
   (loop [res vals oprs (rest opr-vec)]
      (let [opr (first oprs)
            rest (rest oprs)]
        (if (= (count oprs) 0)
          (first res)
          (recur [(apply opr res)] rest))))))

(defn filter-check
  [filters row]
  (loop [filters filters]
    (let [filter (first filters)
          rem (rest filters)]
      (if (= filter nil)
        true
        (if (not= (filter row) true)
          false
          (recur rem))))))

(defn toInt
  [string]
  (Integer/parseInt string))

(defn toDouble
  [string]
  (Double/parseDouble string))

(defn toString
  [string]
  string)

(defn toDate
  [string]
  (Date. string))

(def operation-type-map
  {toInt "int"
   toDouble "double"
   toString "string"
   toDate "date"})
