(ns clojask.utils
  (:require [clojure.core.async :refer [chan sliding-buffer >!! close!]]
            [clojure.java.io :refer [resource]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [tech.v3.dataset :as ds])
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

(def type-operation-map
  {"Integer" toInt
   "float64" toDouble
   "string" toString
   "packed-local-date" toDate})

(defn get-key
  "Gets value of key from a map"
  [key row col-info]
  (if (contains? (:col-type col-info) key)
    ((key (:col-type col-info)) (key row))
    (key row)))

(defn type-detection
 [file]
 (let [sample (take 5 file)]
   ))
