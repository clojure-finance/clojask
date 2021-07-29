(ns clojask.utils
  (:require [clojure.core.async :refer [chan sliding-buffer >!! close!]]
            [clojure.java.io :refer [resource]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [tech.v3.dataset :as ds]
            )
  (:import (java.util Date)))
"Utility function used in dataframe"

(defn get-key
  [row types key-index key]
  (let [index (get key-index key)]
    (if (contains? types index)
      ((get types index) (.get row index))
      (.get row index))))

(defn get-val
  [row types index]
  (map (fn [_] (if (contains? types _)
                 ((get types _) (nth row _))
                 (nth row _)))
       index))

(defn eval-res
  [row types operations index]
  ;; (spit "resources/debug.txt" (str row "\n") :append true)
  ;; (spit "resources/debug.txt" (str types) :append true)
  ;; (spit "resources/debug.txt" operations :append true)
  ;; (spit "resources/debug.txt" (str index "\n") :append true)
  ;; (println opr-vec)
  (let [opr-vec (get operations index)
        vals (get-val row types (first opr-vec))]
    ;; (println [vals])
    (loop [res vals oprs (rest opr-vec)]
      (let [opr (first oprs)
            rest (rest oprs)]
        (if (= (count oprs) 0)
          (first res)
          (recur [(apply opr res)] rest))))))

(defn eval-res-ne
  [row types operations index]
  ;; (spit "resources/debug.txt" (str row "\n") :append true)
  ;; (spit "resources/debug.txt" (str types) :append true)
  ;; (spit "resources/debug.txt" operations :append true)
  ;; (spit "resources/debug.txt" (str index "\n") :append true)
  ;; (println opr-vec)
  (try
    (let [opr-vec (get operations index)
        vals (get-val row types (first opr-vec))]
    ;; (println [vals])
    (loop [res vals oprs (rest opr-vec)]
      (let [opr (first oprs)
            rest (rest oprs)]
        (if (= (count oprs) 0)
          (first res)
          (recur [(apply opr res)] rest)))))
    (catch Exception e nil)))

(defn filter-check
  [filters types row]
  ;; (loop [filters filters]
  ;;   (let [filter (first filters)
  ;;         rem (rest filters)]
  ;;     (if (= filter nil)
  ;;       true
  ;;       (if (not= (filter row) true)
  ;;         false
  ;;         (recur rem)))))
  (if (= filters [])
    true
    (loop [filters filters]
      (let [com (first filters)
            rem (rest filters)]
        ;; (println com)
        (if (= com nil)
          true
          (do
            ;; (println row)
            ;; (println (nth com 1))
            (if (apply (first com) (get-val row types (nth com 1)))
              (recur rem)
              false)))))))

(defn toInt
  [string]
  (if (not= string "")
    (Integer/parseInt string)
    nil))

(defn toDouble
  [string]
  (if (not= string "")
    (Double/parseDouble string)
    nil))

(defn toString
  [string]
  string)

(defn toDate
  [string]
  (if (not= string "")
    ;; (Date. string)
    (.parse (java.text.SimpleDateFormat. "yyyy-MM-dd") string)
    nil))

;; (def operation-type-map
;;   {toInt "int"
;;    toDouble "double"
;;    toString "string"
;;    toDate "date"})

(def type-operation-map
  {"int" toInt
   "double" toDouble
   "string" toString
   "date" toDate})

(defn type-detection
 [file]
 (let [sample (take 5 file)]
   ))
