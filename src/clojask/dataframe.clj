(ns clojask.DataFrame
  (:require [clojask.ColInfo :refer [->ColInfo]]
            [clojask.RowInfo :refer [->RowInfo]]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojask.utils :refer :all]
            [clojask.groupby :refer [internal-aggregate aggre-min]]
            [clojask.onyx-comps :refer [start-onyx start-onyx-groupby]]
            [clojask.sort :as sort]
            [clojask.join :as join]
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
  (addParser [parser col] "add the parser for a col which acts like setType")
  (colDesc [])
  (colTypes [])
  (groupby [a] "group the dataframe by the key(s)")
  (aggregate [a c b] "aggregate the group-by result by the function")
  (head [n])
  (filter [cols predicate])
  (computeAggre [^int num-worker ^String output-dir ^boolean exception])
  (sort [a b] "sort the dataframe based on columns")
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
    (assert (= java.lang.String (type newCol)) "new column should be a string")
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
   [this cols predicate]
   (let [cols (if (coll? cols)
                cols
                (vector cols))
         indices (map (fn [_] (get (.getKeyIndex (:col-info this)) _)) cols)]
     (assert (not (.contains indices nil)) "Some columns do not exist")
     (.filter row-info indices predicate)))
  (colDesc
    [this]
    (.getDesc col-info))
  (colTypes
    [this]
    (.getType col-info))
  (head
    [this n]
    (with-open [reader (io/reader path)]
      (doall (take n (csv/read-csv reader)))))
  (setType
    [this type colName]
    (let [opr (get type-operation-map type)]
      (if (= opr nil)
        "No such type. You could instead write your parsing function as the first operation to this column."
        (.setType col-info opr colName))))
  (addParser
   [this parser colName]
   (.setType col-info parser colName))
    ;; currently put read file here
  (compute
  ;;  [this & {:keys [num-worker output-dir] :or {num-worker 1 output-dir "resources/test.csv"}}]
    [this ^int num-worker ^String output-dir ^boolean exception]
  ;;  "success"))
    (if (<= num-worker 8)
      ;; (try
      ;;   (with-open [rdr (io/reader path) wtr (io/writer  output-dir)]
      ;; ;; (with-open [rdr (io/reader path) wtr (io/output-stream "test.txt")]
      ;;     (let [o-keys (map keyword (first (csv/read-csv rdr)))
      ;;           keys (.getKeys col-info)]
      ;;       (.write wtr (str (clojure.string/join "," (map name keys)) "\n"))
      ;;       (if exception
      ;;         (doseq [line (csv/read-csv rdr)]
      ;;           (let [row (zipmap o-keys line)]
      ;;             (if (filter-check (.getFilters row-info) row)
      ;;               (do
      ;;                 (doseq [key keys]
      ;;                   (.write wtr (str (eval-res row (key (.getDesc col-info)))))
      ;;                   (if (not= key (last keys)) (.write wtr ",")))
      ;;                 (.write wtr "\n")))))
      ;;         (doseq [line (csv/read-csv rdr)]
      ;;           (let [row (zipmap o-keys line)]
      ;;             (if (filter-check (.getFilters row-info) row)
      ;;               (do
      ;;                 (doseq [key keys]
      ;;                   (try
      ;;                     (.write wtr (str (eval-res row (key (.getDesc col-info)))))
      ;;                     (catch Exception e nil))
      ;;                   (if (not= key (last keys)) (.write wtr ",")))
      ;;                 (.write wtr "\n")))))))
      ;;     (.flush wtr)
      ;;     "success")
      ;;   (catch Exception e e))
      (try
        (let [res (start-onyx num-worker batch-size this output-dir exception)]
          (if (= res "success")
            "success"
            "failed"))
        (catch Exception e e))
      "Max worker node number is 8."))
  (computeAggre
    [this ^int num-worker ^String output-dir ^boolean exception]
    (if (<= num-worker 8)
      (try
        (let [res (start-onyx-groupby num-worker batch-size this "_clojask/grouped/" (.getGroupbyKeys (:row-info this)) exception)]
          (if (= res "success")
          ;;  (if (= "success" (start-onyx-aggre num-worker batch-size this output-dir (.getGroupbyKeys (:row-info this)) exception))
            (if (internal-aggregate (.getAggreFunc (:row-info this)) output-dir (.getKeyIndex col-info) (.getGroupbyKeys (:row-info this)) (.getAggreOldKeys (:row-info this)) (.getAggreNewKeys (:row-info this)))
              "success"
              "failed at aggregate stage")
            "failed at group by stage"))
        (catch Exception e e))
      "Max worker node number is 8."))
  (sort
    [this list output-dir]
    (assert (and (not (empty? list)) (loop [list list key false]
                                       (if (and (not key) (= list '()))
                                         true
                                         (let [_ (first list)
                                               res (rest list)]
                                           (if key
                                             (if (and (contains? (.getKeyIndex col-info) _) (= (type _) java.lang.String))
                                               (recur res false)
                                               false)
                                             (if (or (= _ "+") (= _ "-"))
                                               (recur res true)
                                               false))))))
            "The order list is not in the correct format.")
    (defn clojask-compare
      "a and b are two rows"
      [a b]
      (loop [list list key false sign +]
        (if (= list '())
          0
          (let [_ (first list)
                res (rest list)]
            (if key
              (let [tmp (compare (get-key a (.getType col-info) (.getKeyIndex col-info) _) (get-key b (.getType col-info) (.getKeyIndex col-info) _))]
                (if (not= tmp 0)
                  (sign tmp)
                  (recur res false nil)))
              (if (= _ "+")
                (recur res true +)
                (recur res true -)))))))
    (sort/use-external-sort path output-dir clojask-compare)))

(defn dataframe
  [path]
  (try
    (let [reader (io/reader path)
          file (csv/read-csv reader)
          colNames (doall (first file))
          ;; colNames ["test"]
          col-info (ColInfo. (doall (map keyword colNames)) {} {} {} {})
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
  [this cols predicate]
  (.filter this cols predicate))

(defn operate
  ([this operation colName]
   (.operate this operation colName))
  ([this operation colName newCol]
   (.operate this operation colName newCol)))

(defn compute
  [this num-worker output-dir & {:keys [exception] :or {exception false}}]
  ;; delete the files in output-dir and /_clojask/grouped
  (io/delete-file output-dir true)
  ;; (io/delete-file "./_clojask/grouped" true)
  (doseq [file (rest (file-seq (clojure.java.io/file "./_clojask/grouped/")))]
    (io/delete-file file))
  (io/make-parents "./_clojask/grouped/a.txt")
  (if (= (.getAggreOldKeys (:row-info this)) nil)
    (.compute this num-worker output-dir exception)
    (.computeAggre this num-worker output-dir exception)))

(defn group-by
  [this key]
  (.groupby this key))

(defn aggregate
  [this func old-key & [new-key]]
  (.aggregate this func old-key new-key))

(defn sort
  [this list output-dir]
  (.sort this list output-dir))

(defn set-type
  [this type col]
  (.setType this type col))

(defn add-parser
  [this parser col]
  (.addParser this parser col))

(defn inner-join
  [a b a-keys b-keys]
  (assert (and (= (type b) clojask.DataFrame.DataFrame) (= (type a) clojask.DataFrame.DataFrame)) "First two arguments should be clojask dataframes.")
  (assert (= (count a-keys) (count b-keys)) "The length of left keys and right keys should be equal.")
  (join/internal-inner-join a b a-keys b-keys))
