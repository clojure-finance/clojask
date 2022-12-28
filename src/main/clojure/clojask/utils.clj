(ns clojask.utils
  (:require [clojure.core.async :refer [chan sliding-buffer >!! close!]]
            [clojure.java.io :refer [resource]]
            [onyx.plugin.core-async :refer [take-segments!]]
            ;; [tech.v3.dataset :as ds]
            [clojure.string :as str]
            [clojure.java.io :as io])
  (:import (java.util Date)
           (java.time LocalDate)
           (java.time LocalDateTime)
           (java.time.format DateTimeFormatter)
           (java.util Base64)))
"Utility function used in dataframe"

(defn gets
  "unlike core/get, get elements from indices"
  [coll indices]
  (mapv #(nth coll %) indices)
  )

(defn gets-format
  "gets with format"
  [coll indices formatters]
  (mapv (fn [_] 
          (let [val (nth coll _)]
            (if-let [formatter (get formatters _)]
              (formatter val)
              val))) indices)
  )

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
  [row types formats operations index]
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
  [row types formats operations index]
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

(def fromString
  (atom (fn [_] (str _))))

(def toDate
  (atom (fn [string]
          (try
            (.parse (java.text.SimpleDateFormat. "yyyy-MM-dd") string)
            ;; (catch Exception e (throw e))
            (catch Exception e nil)
            ))))

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
                  (catch Exception e (throw e))
                  (catch Exception e nil)
                  )))

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
                  ;; (catch Exception e (throw e))
                  (catch Exception e nil)
                  )))

      (reset! fromDate
              (fn [date]
                (if (= (type date) java.util.Date)
                  (.format (java.text.SimpleDateFormat. "yyyy-MM-dd") date)
                  date))))))

(def type-operation-map
  {"int" [toInt fromString]
   "double" [toDouble fromString]
   "string" [toString fromString]
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

(defn init-file
  [out-dir header]
  (if (not= out-dir nil)
   (io/delete-file out-dir true))
  (doseq [file (rest (file-seq (io/file "./.clojask/grouped/")))]
    (try
      (io/delete-file file)
      (catch Exception e nil)))
  (doseq [file (rest (file-seq (io/file "./.clojask/join/")))]
    (try
      (io/delete-file file)
      (catch Exception e nil)))
  (io/make-parents "./.clojask/grouped/a.txt")
  (io/make-parents "./.clojask/join/a/a.txt")
  (io/make-parents "./.clojask/join/b/a.txt")
  (io/make-parents "./.clojask/sort/a.txt")
  ;; (if (not= header nil)
  ;;   (with-open [wrtr (io/writer out-dir)]
  ;;     (.write wrtr (str (str/join "," header) "\n"))))
  )

(defn get-type-string
  [x]
  (if (not= x nil)
   (subs (str (type x)) 6)
    "nil"))

(defn get-type-string-vec
  [col]
  (let [types (mapv get-type-string col)
        types (sort (vec (set types)))]
    (str/join " & " types)))

(defn check-duplicate-col
  "Check for duplicated column names and return a column names list w/o duplicates"
  [colNames]
  (if (not= (count (distinct colNames)) (count colNames))
    (do
      (println "WARNING: Duplicated columns found")
      (let [colNames-var (atom colNames)
            duplicate-list (into (sorted-map) (clojure.core/filter #(> (last %) 1) (frequencies (deref colNames-var))))
            counter (atom {})]
        (doseq [duplicate-col duplicate-list]
          (swap! counter assoc (first duplicate-col) (atom 0)))
        (doseq [col colNames]
          (if (contains? duplicate-list col)
            (reset! colNames-var (map #(if (= % col)
                                         (do
                                           (swap! (get @counter col) inc)
                                           (str % (deref (get @counter col))))
                                         %) (deref colNames-var)))))
        (deref colNames-var)))
    colNames))

(defn proc-groupby-key-each
  [pair]
  (if (coll? pair)
    (if (and (= 2 (count pair)) (fn? (first pair)) (string? (nth pair 1)))
      pair
      (if (and (= 1 (count pair)) (string? (first pair)))
        [nil pair]
        (throw (Exception.))))
    (if (string? pair)
      [nil pair]
      (throw (Exception.)))))

(defn proc-groupby-key
  [input]
  (try
    (if (coll? input)
    ;; it is a collection
      (if (fn? (first input))
        (if (= 2 (count input))
          [input]
          nil)
        (mapv proc-groupby-key-each input))
      (if (string? input)
        [[nil input]]
        nil))
    (catch Exception e nil)))

(defn get-func-str
  [func]
  (let [func-str (str func)]
    (str/replace (str/replace (subs func-str  0 (str/last-index-of func-str "@")) "$" "/") "_" "-")))

(def encoder (Base64/getUrlEncoder))
(def decoder (Base64/getUrlDecoder))

(defn encode-str
  [s]
  (.encodeToString encoder (.getBytes s)))

(defn decode-str
  [s]
  (String. (.decode decoder s)))

;; (def toDate
;;   (atom (fn [string]
;;           (try
;;             (LocalDate/parse string (DateTimeFormatter/ofPattern "yyyy-MM-dd"))
;;             (catch Exception e (throw e))))))

;; (def fromDate
;;   (atom (fn [date]
;;           (if (= (type date) java.time.LocalDate)
;;             (.format date (DateTimeFormatter/ofPattern "yyyy-MM-dd"))
;;             date))))

;; (def toDateTime
;;   (atom (fn [string]
;;           (try
;;             (LocalDateTime/parse string (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss"))
;;             (catch Exception e (throw e))))))

;; (def fromDateTime
;;   (atom (fn [date]
;;           (if (= (type date) java.time.LocalDateTime)
;;             (.format date (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss"))
;;             date))))

;; (defn set-format-string
;;   [string]
;;   (if (or (str/starts-with? string "date:") (str/starts-with? string "datetime:"))
;;     (let [format-string (subs string (inc (str/index-of string ":")))]
;;       (reset! toDate
;;               (fn [string]
;;                 (try
;;                   (LocalDate/parse string (DateTimeFormatter/ofPattern format-string))
;;                   (catch Exception e (throw e)))))

;;       (reset! fromDate
;;               (fn [date]
;;                 (if (= (type date) java.time.LocalDate)
;;                   (.format date (DateTimeFormatter/ofPattern format-string))
;;                   date)))

;;       (reset! toDateTime
;;               (fn [string]
;;                 (try
;;                   (LocalDateTime/parse string (DateTimeFormatter/ofPattern format-string))
;;                   (catch Exception e (throw e)))))

;;       (reset! fromDateTime
;;               (fn [date]
;;                 (if (= (type date) java.time.LocalDateTime)
;;                   (.format date (DateTimeFormatter/ofPattern format-string))
;;                   date))))
;;     ))

;; ;; (def operation-type-map
;; ;;   {toInt "int"
;; ;;    toDouble "double"
;; ;;    toString "string"
;; ;;    toDate "date"})

;; (def type-operation-map
;;   {"int" [toInt fromString]
;;    "double" [toDouble fromString]
;;    "string" [toString fromString]
;;    "date" [toDate fromDate]
;;    "datetime" [toDateTime fromDateTime]})