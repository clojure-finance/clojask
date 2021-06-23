(ns clojask.DataFrame
  (:require [clojask.ColInfo :refer [->ColInfo]]
            [clojask.RowInfo :refer [->RowInfo]]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojask.utils :refer :all]
            [clojask.groupby :refer [internal-aggregate aggre-min]]
            [clojask.onyx-comps :refer [start-onyx start-onyx-aggre start-onyx-groupby]]
            )
  (:import [clojask.ColInfo ColInfo]
           [clojask.RowInfo RowInfo]))
"The clojask lazy dataframe"

(definterface DFIntf
  ;; (compute [& {:keys [num-worker output-dir] :or {num-worker 1 output-dir "resources/test.csv"}}])
  (compute [^int num-worker ^String output-dir ^boolean exception])
  (operate [operation colName] "operate an operation to column and replace in place")
  (operate [operation colName newCol] "operate an operation to column and add the result as new column")
  (setType [type colName] "types supported: int double string date")
  (colDesc [])
  (colTypes [])
  (groupby [a] "group the dataframe by the key(s)")
  (aggregate [a c b] "aggregate the group-by result by the function")
  (head [n])
  (filter [predicate])
  (computeAggre [^int num-worker ^String output-dir ^boolean exception])
  (assoc [dataframe])  ;; combine another dataframe with the current
  )

;; each dataframe can have a delayed object
(defrecord DataFrame
           [^String path
            ^Integer batch-size
   ;;  ...
   ;; more fields to add
   ;;  ...
            ^ColInfo col-info
            ^RowInfo row-info]
  DFIntf
  (operate
    [this operation colName]
    (.operate col-info operation colName))
  (operate
    [this operation colNames newCol]
    (assert (= clojure.lang.Keyword (type newCol)))
    (.operate col-info operation colNames newCol))
  (groupby
    [this key]
    (let [keys (if (coll? key)
                 key
                 [key])]
      (.groupby row-info keys)))
  (aggregate
    [this func old-key new-key]
    (.aggregate row-info func old-key new-key))
  (filter
    [this predicate]
    (.filter row-info predicate))
  (colDesc
    [this]
    (.getDesc col-info))
  (colTypes
    [this]
    (.getType col-info))
  (head
    [this n]
    ())
  (setType
    [this type colName]
    (case type
      "int" (.setType col-info toInt colName)
      "double" (.setType col-info toDouble colName)
      "string" (.setType col-info toString colName)
      "date" (.setType col-info toDate colName)
      "No such type. You could instead write your parsing function as the first operation to this column."))
    ;; currently put read file here
  (compute
  ;;  [this & {:keys [num-worker output-dir] :or {num-worker 1 output-dir "resources/test.csv"}}]
    [this ^int num-worker ^String output-dir ^boolean exception]
  ;;  "success"))
    (if (<= num-worker 1)
      (try
        (with-open [rdr (io/reader path) wtr (io/writer  output-dir)]
      ;; (with-open [rdr (io/reader path) wtr (io/output-stream "test.txt")]
          (let [o-keys (map keyword (first (csv/read-csv rdr)))
                keys (.getKeys col-info)]
            (.write wtr (str (clojure.string/join "," (map name keys)) "\n"))
            (if exception
              (doseq [line (csv/read-csv rdr)]
                (let [row (zipmap o-keys line)]
                  (if (filter-check (.getFilters row-info) row)
                    (do
                      (doseq [key keys]
                        (.write wtr (str (eval-res row (key (.getDesc col-info)))))
                        (if (not= key (last keys)) (.write wtr ",")))
                      (.write wtr "\n")))))
              (doseq [line (csv/read-csv rdr)]
                (let [row (zipmap o-keys line)]
                  (if (filter-check (.getFilters row-info) row)
                    (do
                      (doseq [key keys]
                        (try
                          (.write wtr (str (eval-res row (key (.getDesc col-info)))))
                          (catch Exception e nil))
                        (if (not= key (last keys)) (.write wtr ",")))
                      (.write wtr "\n")))))))
          (.flush wtr)
          "success")
        (catch Exception e e))
      (try
        (let [res (start-onyx num-worker batch-size this output-dir exception)]
          (if (= res "success")
            "success"
            "failed"))
        (catch Exception e e))))
  (computeAggre
   [this ^int num-worker ^String output-dir ^boolean exception]
   (if (< num-worker 2)
     nil
     (try
       (let [res (start-onyx-groupby num-worker batch-size this output-dir (.getGroupbyKeys (:row-info this)) exception)]
         (if (= res "success")
          ;;  (if (= "success" (start-onyx-aggre num-worker batch-size this output-dir (.getGroupbyKeys (:row-info this)) exception))
           (if (internal-aggregate (.getAggreFunc (:row-info this)) output-dir (.getGroupbyKeys (:row-info this)) (.getAggreOldKeys (:row-info this)) (.getAggreNewKeys (:row-info this)))
             "success"
             "failed at aggregate stage")
           "failed at group by stage"))
       (catch Exception e e)))))

(defn dataframe
  [path]
  (try
    (let [reader (io/reader path)
          file (csv/read-csv reader)
          colNames (doall (first file))
          ;; colNames ["test"]
          col-info (ColInfo. (doall (map keyword colNames)) {} {})
          row-info (RowInfo. [] [] nil nil nil)]
      ;; (type-detection file)
      (.close reader)
      (.init col-info colNames)

      ;; 
      ;; type detection
      ;; 

      (DataFrame. path 10 col-info row-info))
    (catch Exception e
      (do
        (println "No such file or directory")
        nil))))

(defn filter
  [this predicate]
  (.filter this predicate))

(defn operate
  ([this operation colName]
   (.operate this operation colName))
  ([this operation colName newCol]
   (.operate this operation colName newCol)))

(defn compute
  [this num-worker output-dir & {:keys [exception] :or {exception false}}]
  ;; delete the files in output-dir and /_grouped
  (io/delete-file output-dir true)
  (io/delete-file "./_grouped" true)
  (io/make-parents "./_grouped/a.txt")
  (if (= (.getAggreOldKeys (:row-info this)) nil)
    (.compute this num-worker output-dir exception)
    (.computeAggre this num-worker output-dir exception)))

(defn group-by
  [this key]
  (.groupby this key))

(defn aggregate
  [this func old-key & [new-key]]
  (.aggregate this func old-key new-key))