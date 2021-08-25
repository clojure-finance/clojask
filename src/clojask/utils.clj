(ns clojask.utils
  (:require [clojure.core.async :refer [chan sliding-buffer >!! close!]]
            [clojure.java.io :refer [resource]]
            [onyx.plugin.core-async :refer [take-segments!]]
            [tech.v3.dataset :as ds]
            [clojure.string :as str])
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
  (map (fn [_] (if-let [parser (get types _)]
                 (parser (nth row _))
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
      (if (= (count oprs) 0)
        (first res)
        (let [opr (first oprs)
              rest (rest oprs)]
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
        (if (= (count oprs) 0)
          (first res)
          (let [opr (first oprs)
                rest (rest oprs)]
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

(def toInt
  (atom (fn [string]
          (try
            (Integer/parseInt string)
            (catch Exception e nil)))))

(def toDouble
  (atom (fn [string]
          (try
            (Double/parseDouble string)
            (catch Exception e nil)))))

(def toString
  (atom (fn [string]
          string)))

(def toDate
  (atom (fn [string]
          (try
            (.parse (java.text.SimpleDateFormat. "yyyy-MM-dd") string)
            (catch Exception e (throw e))))))

(def fromDate
  (atom (fn [date]
          (if (= (type date) java.util.Date)
            (.format (java.text.SimpleDateFormat. "yyyy-MM-dd") date)
            date))))

(defn set-format-string
  [string]
  (if (or (str/starts-with? string "date:") (str/starts-with? string "datetime:"))
    (let [format-string (subs string (inc (str/index-of string ":")))]
      (reset! toDate
        (fn [string]
          (try
            (.parse (java.text.SimpleDateFormat. format-string) string)
            (catch Exception e (throw e)))))

      (reset! fromDate
        (fn [date]
          (if (= (type date) java.util.Date)
            (.format (java.text.SimpleDateFormat. format-string) date)
            date))))
    (do
      (reset! toDate
              (fn [string]
                (try
                  (.parse (java.text.SimpleDateFormat. "yyyy-MM-dd") string)
                  (catch Exception e (throw e)))))

      (reset! fromDate
              (fn [date]
                (if (= (type date) java.util.Date)
                  (.format (java.text.SimpleDateFormat. "yyyy-MM-dd") date)
                  date))))))

;; (def operation-type-map
;;   {toInt "int"
;;    toDouble "double"
;;    toString "string"
;;    toDate "date"})

(def type-operation-map
  {"int" [toInt str]
   "double" [toDouble str]
   "string" [toString str]
   "date" [toDate fromDate]
   "datetime" [toDate fromDate]})

(defn type-detection
  [file]
  (let [sample (take 5 file)]))

(defn is-in
  [col dataframe]
  (if (contains? (.getKeyIndex (:col-info dataframe)) col)
    true
    false))

(defn is-out
  [col dataframe]
  (if (contains? (.getKeyIndex (:col-info dataframe)) col)
    false
    true))

(defn are-in
  "return should be [] if all in"
  [cols dataframe]
  (filter (fn [col] (is-out col dataframe)) cols))

(defn are-out
  "return should be [] if all out"
  [cols dataframe]
  (filter (fn [col] (is-in col dataframe)) cols))

(defn max
  [list]
  (reduce (fn [a b] (if (> (compare a b) 0)
                      a
                      b))
          list))

(defn min
  [list]
  (reduce (fn [a b] (if (< (compare a b) 0)
                      a
                      b))
          list))