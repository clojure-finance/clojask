(ns clojask.classes.DataFrame
  (:require [clojure.set :as set]
            [clojask.classes.ColInfo :refer [->ColInfo]]
            [clojask.classes.RowInfo :refer [->RowInfo]]
            [clojask.classes.DataStat :refer [->DataStat]]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojask.utils :as u]
            [clojask.onyx-comps :refer [start-onyx start-onyx-aggre-only start-onyx-groupby start-onyx-join]]
            [clojask.sort :as sort]
            [clojask.aggregate.aggre-onyx-comps :refer [start-onyx-aggre]]
            [clojask.join.outer-onyx-comps :refer [start-onyx-outer]]
            [clojure.string :as str]
            [clojask.preview :as preview]
            [clojure.pprint :as pprint]
            [clojask.classes.DataStat :refer [compute-stat]]
            [clojask-io.input :refer [read-file]]
            [clojask.join.outer-output :as output])
  (:import [clojask.classes.ColInfo ColInfo]
           [clojask.classes.RowInfo RowInfo]
           [clojask.classes.DataStat DataStat]
           [com.clojask.exception TypeException OperationException])
  (:refer-clojure :exclude [filter group-by sort]))

"The clojask lazy dataframe"

(def debug (atom false))

(definterface GenDFIntf
  (checkInputPathClash [path] "check if paths clashes with dataframes a/b input path")
  (getColNames [] "get the names of all the columns")
  (printCol [output-path selected-col out] "print column names to output file")
  (setOutput [output])
  (getOutput [])
  (preview [sample-size output-size format] "quickly return a vector of maps about the resultant dataframe"))

(definterface DFIntf
  (getPath [] "get input path of dataframe")
  (checkOutputPath [output-path] "check if output path is of string type")
  (operate [operation colName] "operate an operation to column and replace in place")
  (operate [operation colName newCol] "operate an operation to column and add the result as new column")
  (setType [type colName] "types supported: int double string date")
  (setParser [parser col] "add the parser for a col which acts like setType")
  (colDesc [] "get column description in ColInfo")
  (colTypes [] "get column type in ColInfo")
  (getColIndex [] "get column indices, excluding deleted columns")
  (getAggreColNames [] "get column names if there is aggregate")
  (getStat [] "get the statistics of the dataframe")
  (printCol [output-path selected-index out] "print column names to output file")
  (delCol [col-to-del] "delete one or more columns in the dataframe")
  (reorderCol [new-col-order] "reorder columns in the dataframe")
  (renameCol [old-col new-col] "rename columns in the dataframe")
  (groupby [a] "group the dataframe by the key(s)")
  (aggregate [a c b] "aggregate the group-by result by the function")
  (head [n] "return first n lines in dataframe")
  (filter [cols predicate])
  (computeTypeCheck [num-worker output-dir])
  (compute [^int num-worker ^String output-dir ^boolean exception ^boolean order select melt ifheader out])
  (computeGroupAggre [^int num-worker ^String output-dir ^boolean exception select ifheader out])
  (computeAggre [^int num-worker ^String output-dir ^boolean exception select ifheader out])
  (sort [a b] "sort the dataframe based on columns")
  (addFormatter [a b] "format the column as the last step of the computation")
  (previewDF [] "preview function used for error predetection")
  (errorPredetect [msg] "prints exception with msg if error is detected in preview"))

;; each dataframe can have a delayed object
(defrecord DataFrame
           [^String path
            ^Integer batch-size
            ^ColInfo col-info
            ^RowInfo row-info
            ^DataStat stat
            output-func
            ^Boolean have-col]
  
  GenDFIntf

  (checkInputPathClash
    [this path]
    (defn get-path-str
      [path]
      (if (str/starts-with? path "./")
        (str "file:///" (str/replace-first path "./" ""))
        (if (str/starts-with? path "/")
          (str "file:///" (str/replace-first path "/" ""))
          (str "file:///" path))))
    (let [path-str (get-path-str path)
          input-path-str (get-path-str (.getPath this))
          path-obj (java.nio.file.Paths/get (new java.net.URI path-str))
          input-path-obj (java.nio.file.Paths/get (new java.net.URI input-path-str))
          paths-equal (java.nio.file.Paths/.equals path-obj input-path-obj)]
      (cond paths-equal
            (throw (OperationException. "Output path should be different from input path of dataframe argument.")))))

  (getOutput
    [this]
    (deref output-func))

  (setOutput
    [this output]
    (reset! output-func output))
  
  (getColNames
    [this]
    (if (and (= 0 (count (.getGroupbyKeys (:row-info this)))) (= 0 (count (.getAggreNewKeys (:row-info this)))))
        ;; not aggregate
      (let [index-key (.getIndexKey (:col-info this))
            index (.getColIndex this)]
        (mapv (fn [i] (get index-key i)) index))
        ;; if aggregate
      (.getAggreColNames this)))
  
  (printCol
    ;; print column names, called by compute, computeAggre and computeGroupByAggre
    [this output-path selected-index out]
    (let [col-set (if (= selected-index [nil]) (.getColNames this) (mapv (vec (.getColNames this)) selected-index))]
      (let [wrtr (if output-path (io/writer output-path) nil)]
        ((or out (.getOutput this)) wrtr [col-set])
        (if output-path (.close wrtr)))))
  
  (preview
    [this sample-size return-size format]
    (cond (not (and (integer? sample-size) (integer? return-size)))
          (throw (TypeException. "Arguments passed to preview must be integers.")))
    (preview/preview this sample-size return-size format))

  (compute
    [this ^int num-worker ^String output-dir ^boolean exception ^boolean order select melt ifheader out]
    ;(assert (= java.lang.String (type output-dir)) "output path should be a string")
    (let [key-index (.getKeyIndex (:col-info this))
          select (if (coll? select) select [select])
          index (if (= select [nil]) (take (count key-index) (iterate inc 0)) (vals (select-keys key-index select)))]
      (assert (or (= (count select) (count index)) (= select [nil])) (OperationException. "Must select existing columns. You may check it using"))
      ;; (if (<= num-worker 8)
      (if true
        (do
          (if (= ifheader nil) (.printCol this output-dir index out))
          (let [res (start-onyx num-worker batch-size this output-dir exception order index melt out)]
            (if (= res "success")
              "success"
              "failed")))
        (throw (OperationException. "Max number of worker nodes is 8.")))))

  DFIntf

  (getPath
    [this]
    path)

  (checkOutputPath
    [this output-path]
    (cond (not (= java.lang.String (type output-path)))
          (throw (TypeException. "Output path should be a string."))))


  (operate ;; has assert
    [this operation colName]
    (if (nil? (.operate col-info operation colName))
      this ; "success"
      (throw (OperationException. "operate"))))

  (operate
    [this operation colNames newCol]
    (cond (not (= java.lang.String (type newCol)))
          (throw (TypeException.  "New column should be a string.")))
    (if (nil? (.operate col-info operation colNames newCol))
      this
      (throw (OperationException. "Error in running operate."))))

  (groupby
    [this key]
    (let [input (u/proc-groupby-key key)
          keys (map #(nth % 1) input)]
      (cond (not (not= input nil))
            (throw (TypeException. "The group-by keys format is not correct.")))
      (cond (not (= 0 (count (u/are-in keys this))))
            (throw (TypeException. "Input includes non-existent column name(s).")))
      (let [keys (mapv (fn [_] [(first _) (get (.getKeyIndex col-info) (nth _ 1))]) input)]
        (if (nil? (.groupby row-info keys))
          this
          (throw (OperationException. "groupby"))))))

  (aggregate
    [this func old-key new-key]
    (cond (not (= 0 (count (u/are-in old-key this))))
          (throw (TypeException. "Input includes non-existent column name(s).")))
    (cond (not (= 0 (count (u/are-out new-key this))))
          (throw (TypeException. "New keys should not be existing column names.")))
    (let [old-key (mapv (fn [_] (get (.getKeyIndex col-info) _)) old-key)]
      (if (nil? (.aggregate row-info func old-key new-key))
        this
        (throw (OperationException. "aggregate")))))

  (filter
    [this cols predicate]
    (let [cols (if (coll? cols)
                 cols
                 (vector cols))
          indices (map (fn [_] (get (.getKeyIndex (:col-info this)) _)) cols)]
      (cond (not (u/are-in cols this))
            (throw (TypeException. "Input includes non-existent column name(s).")))
      (if (nil? (.filter row-info indices predicate))
        this
        (throw (OperationException. "filter")))))

  (colDesc
    [this]
    (.getDesc col-info))

  (colTypes
    [this]
    (.getType col-info))

  (getColIndex
    [this]
    (let [index-key (.getIndexKey (:col-info this))]
      (take (count index-key) (iterate inc 0))))

  (getStat
    [this]
    (:stat this))

  (getAggreColNames  ;; called by getColNames and preview
    [this]
    (let [index-key (.getIndexKey (.col-info this))
          groupby-key-index (.getGroupbyKeys (:row-info this))
          groupby-keys (.getGroupbyKeys (:row-info this))
          groupby-keys-value (vec (map #(if (nth % 0)
                                          (let [func-str (str (nth % 0))
                                                tmp-idx (+ (str/index-of func-str "$") 1)
                                                bgn-idx (+ (str/index-of func-str "$" tmp-idx) 1)
                                                end-idx (str/index-of func-str "__" bgn-idx)
                                                col-func-str (subs func-str bgn-idx end-idx)]
                                            (str col-func-str "(" (index-key (nth % 1)) ")"))
                                          (index-key (nth % 1))) groupby-keys))
        ;; Deprecated
        ;groupby-keys (vec (map (.getIndexKey (.col-info this)) (vec (map #(last %) groupby-key-index))))
          aggre-new-keys (.getAggreNewKeys (:row-info this))]
      (concat groupby-keys-value aggre-new-keys)))

  ;; deprecated
  (delCol
    [this col-to-del]
    (cond (not (= 0 (count (u/are-in col-to-del this))))
          (throw (TypeException. "Input includes non-existent column name(s).")))
    (.delCol (.col-info this) col-to-del)
    ;; "success"
    this)
  ;; deprecated
  (reorderCol
    [this new-col-order]
    (cond (not (= (set (.getKeys (.col-info this))) (set new-col-order)))
          (throw (TypeException. "Set of input in reorder-col contains column(s) that do not exist in dataframe.")))
    (.setColInfo (.col-info this) new-col-order)
    (.setRowInfo (.row-info this) (.getDesc (.col-info this)) new-col-order)
    ;; "success"  
    this)

  (renameCol
    [this old-col new-col]
    (cond (not (u/is-in old-col this))
          (throw (TypeException. "Input includes non-existent column name(s).")))
    ;; (cond (not (= (count (.getKeys (.col-info this))) (count new-col)))
    ;;       (throw (TypeException. "Number of new column names not equal to number of existing columns.")))
    (.renameColInfo (.col-info this) old-col new-col)
    ;; "success"
    this)

  (head
    [this n]
    (cond (not (integer? n))
          (throw (TypeException. "Argument passed to head should be an integer.")))
    (with-open [reader (io/reader path)]
      (doall (take n (csv/read-csv reader)))))

  (setType
    [this type colName]
    (u/set-format-string type)
    (let [type (subs type 0 (if-let [tmp (str/index-of type ":")] tmp (count type)))
          oprs (get u/type-operation-map type)
          parser (deref (nth oprs 0))
          format (deref (nth oprs 1))]
      (if (= oprs nil)
        "No such type. You could instead write your parsing function as the first operation to this column."
        (do
          (.setType col-info parser colName)
          (.addFormatter this format colName)
          ;; "success"
          this))))

  (setParser
    [this parser colName]
    (cond (not (u/is-in colName this))
          (throw (TypeException. "Input includes non-existent column name(s).")))
    (if (nil? (.setType col-info parser colName))
      this
      (throw (OperationException. "Error in running setParser."))))

  (addFormatter
    [this format col]
    (cond (not (u/is-in col this))
          (throw (TypeException. "Input includes non-existent column name(s).")))
    (.setFormatter col-info format col)
    this)

  (computeTypeCheck
    [this num-worker output-dir]
    (cond (not (= java.lang.String (type output-dir)))
          (throw (TypeException. "Output directory should be a string.")))
    (cond (not (integer? num-worker))
          (throw (TypeException. "Number of workers should be an integer.")))
    (if (> num-worker 8)
      (throw (OperationException. "Max number of worker nodes is 8."))))

  (computeAggre
    [this ^int num-worker ^String output-dir ^boolean exception select ifheader out]
    (.computeTypeCheck this num-worker output-dir)
    (let [aggre-keys (.getAggreFunc row-info)
          select (if (coll? select) select [select])
          select (if (= select [nil])
                   (vec (take (count aggre-keys) (iterate inc 0)))
                   (mapv (fn [key] (.indexOf (.getColNames this) key)) select))
          aggre-func (u/gets aggre-keys (vec (apply sorted-set select)))
          select (mapv (fn [num] (count (remove #(>= % num) select))) select)
          index (vec (apply sorted-set (mapv #(nth % 1) aggre-func)))
          shift-func (fn [pair]
                       [(first pair) (let [num (nth pair 1)]
                                       (.indexOf index num))])
          aggre-func (mapv shift-func aggre-func)
          tmp (if (= ifheader nil) (.printCol this output-dir select out))
          res (start-onyx-aggre-only num-worker batch-size this output-dir exception aggre-func index select out)]
      (if (= res "success")
        "success"
        "failed")))

  (computeGroupAggre
    [this ^int num-worker ^String output-dir ^boolean exception select ifheader out]
    (.computeTypeCheck this num-worker output-dir)
    ;; (if (<= num-worker 8)
    (if true
      (let [groupby-keys (.getGroupbyKeys row-info)
            aggre-keys (.getAggreFunc row-info)
            select (if (coll? select) select [select])
            select (if (= select [nil])
                     (vec (take (+ (count groupby-keys) (count aggre-keys)) (iterate inc 0)))
                     (mapv (fn [key] (.indexOf (.getColNames this) key)) select))
              ;; pre-index (remove #(>= % (count groupby-keys)) select)
            data-index (mapv #(- % (count groupby-keys)) (remove #(< % (count groupby-keys)) select))
            groupby-index (vec (apply sorted-set (mapv #(nth % 1) (concat groupby-keys (u/gets aggre-keys data-index)))))
            res (start-onyx-groupby num-worker batch-size this ".clojask/grouped/" groupby-keys groupby-index exception)]
        (if (= aggre-keys [])
          (println (str "Since the dataframe is only grouped by but not aggregated, the result will be the same as to choose the distinct values of "
                        "the groupby keys.")))
        (if (= ifheader nil) (.printCol this output-dir select out))
        (if (= res "success")
          ;;  (if (= "success" (start-onyx-aggre num-worker batch-size this output-dir (.getGroupbyKeys (:row-info this)) exception))
          (let [shift-func (fn [pair]
                             [(first pair) (let [index (nth pair 1)]
                                             (.indexOf groupby-index index))])
                aggre-func (mapv shift-func (u/gets aggre-keys data-index))
                formatter (.getFormatter (.col-info this))
                formatter (set/rename-keys formatter (zipmap groupby-index (iterate inc 0)))]
            (if (= "success" (start-onyx-aggre num-worker batch-size this output-dir exception aggre-func select formatter out))
              "success"
              (throw (OperationException. "Error when aggregating."))))
          (throw (OperationException. "Error when grouping by."))))
      (throw (OperationException. "Max number of worker nodes is 8."))))

  (sort
    [this list output-dir]
    (cond (not (and (not (empty? list)) (loop [list list key false]
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
                                                  false)))))))
          (throw (TypeException. "The order list is not in the correct format.")))
    (defn clojask-compare
      "a and b are two rows"
      [a b]
      (loop [list list key false sign +]
        (if (= list '())
          0
          (let [_ (first list)
                res (rest list)]
            (if key
              (let [tmp (compare (u/get-key a (.getType col-info) (.getKeyIndex col-info) _) (u/get-key b (.getType col-info) (.getKeyIndex col-info) _))]
                (if (not= tmp 0)
                  (sign tmp)
                  (recur res false nil)))
              (if (= _ "+")
                (recur res true +)
                (recur res true -)))))))
    (sort/use-external-sort path output-dir clojask-compare))

  (previewDF
    [this]
    (let [data (.preview this 100 100 false)
          tmp (first data)
          types (zipmap (keys tmp) (map u/get-type-string (vals tmp)))]
      (conj (apply list data) types)))

  (errorPredetect
    [this msg]
    (if (= (deref debug) false)
      (try
        (.previewDF this)
        (catch Exception e
          (do
            (throw (OperationException. (format  (str msg " (original error: %s)") (str (.getMessage e))))))))))

  Object)
