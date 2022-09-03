(ns clojask.dataframe
  (:require [clojure.set :as set]
            [clojure.string :as string]
            [clojask.ColInfo :refer [->ColInfo]]
            [clojask.RowInfo :refer [->RowInfo]]
            [clojask.DataStat :refer [->DataStat]]
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
            [clojask.DataStat :refer [compute-stat]]
            [clojask-io.input :refer [read-file]]
            [clojask.join.outer-output :as output])
  (:import [clojask.ColInfo ColInfo]
           [clojask.RowInfo RowInfo]
           [clojask.DataStat DataStat]
           [com.clojask.exception TypeException OperationException])
  (:refer-clojure :exclude [filter group-by sort]))
"The clojask lazy dataframe"

(def debug (atom false))
(defn enable-debug
  []
  (println "Debug mode is on.")
  (reset! debug true))
(defn disable-debug
  []
  (println "Debug mode is off.")
  (reset! debug false))

(definterface DFIntf
  (compute [^int num-worker ^String output-dir ^boolean exception ^boolean order select melt ifheader out] "final evaluatation")
  (getPath [] "get input path of dataframe")
  (getOutput [] "get the output function of the dataframe")
  (setOutput [output] "set the output function")
  (checkOutputPath [output-path] "check if output path is of string type")
  (checkInputPathClash [path] "check if path clashs with dataframe input path")
  (operate [operation colName] "operate an operation to column and replace in place")
  (operate [operation colName newCol] "operate an operation to column and add the result as new column")
  (setType [type colName] "types supported: int double string date")
  (setParser [parser col] "add the parser for a col which acts like setType")
  (colDesc [] "get column description in ColInfo")
  (colTypes [] "get column type in ColInfo")
  (getColIndex [] "get column indices, excluding deleted columns")
  (getAggreColNames [] "get column names if there is aggregate")
  (getColNames [] "get column names")
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
  (computeGroupAggre [^int num-worker ^String output-dir ^boolean exception select ifheader out])
  (computeAggre [^int num-worker ^String output-dir ^boolean exception select ifheader out])
  (sort [a b] "sort the dataframe based on columns")
  (addFormatter [a b] "format the column as the last step of the computation")
  (preview [sample-size output-size format] "quickly return a vector of maps about the resultant dataframe")
  (previewDF [] "preview function used for error predetection")
  (errorPredetect [msg] "prints exception with msg if error is detected in preview")
  )

;; each dataframe can have a delayed object
(defrecord DataFrame
           [^String path
            ^Integer batch-size
            ^ColInfo col-info
            ^RowInfo row-info
            ^DataStat stat
            output-func
            ^Boolean have-col]
  DFIntf

  (getPath
    [this]
    path)

  (checkOutputPath
    [this output-path]
    (cond (not (= java.lang.String (type output-path)))
          (throw (TypeException. "Output path should be a string."))))

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
                                                tmp-idx (+ (string/index-of func-str "$") 1)
                                                bgn-idx (+ (string/index-of func-str "$" tmp-idx) 1)
                                                end-idx (string/index-of func-str "__" bgn-idx)
                                                col-func-str (subs func-str bgn-idx end-idx)]
                                            (str col-func-str "(" (index-key (nth % 1)) ")"))
                                          (index-key (nth % 1))) groupby-keys))
        ;; Deprecated
        ;groupby-keys (vec (map (.getIndexKey (.col-info this)) (vec (map #(last %) groupby-key-index))))
          aggre-new-keys (.getAggreNewKeys (:row-info this))]
      (concat groupby-keys-value aggre-new-keys)))

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

  (preview
    [this sample-size return-size format]
    (cond (not (and (integer? sample-size) (integer? return-size)))
          (throw (TypeException. "Arguments passed to preview must be integers.")))
    (preview/preview this sample-size return-size format))

  (computeTypeCheck
    [this num-worker output-dir]
    (cond (not (= java.lang.String (type output-dir)))
          (throw (TypeException. "Output directory should be a string.")))
    (cond (not (integer? num-worker))
          (throw (TypeException. "Number of workers should be an integer.")))
    (if (> num-worker 8)
      (throw (OperationException. "Max number of worker nodes is 8."))))

  (compute
    [this ^int num-worker ^String output-dir ^boolean exception ^boolean order select melt ifheader out]
    ;(assert (= java.lang.String (type output-dir)) "output path should be a string")
    (let [key-index (.getKeyIndex (:col-info this))
          select (if (coll? select) select [select])
          index (if (= select [nil]) (take (count key-index) (iterate inc 0)) (vals (select-keys key-index select)))]
      (assert (or (= (count select) (count index)) (= select [nil])) (OperationException. "Must select existing columns. You may check it using"))
      (if (<= num-worker 8)
        (do
          (if (= ifheader nil) (.printCol this output-dir index out))
          (let [res (start-onyx num-worker batch-size this output-dir exception order index melt out)]
            (if (= res "success")
              "success"
              "failed")))
        (throw (OperationException. "Max number of worker nodes is 8.")))))

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
    (if (<= num-worker 8)
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

(defn preview
  [dataframe sample-size return-size & {:keys [format] :or {format false}}]
  (.preview dataframe sample-size return-size format))

(defn generate-col
  "Generate column names if there are none"
  [col-count]
  (vec (map #(str "Col_" %) (range 1 (+ col-count 1)))))

(defn dataframe
  [path & {:keys [if-header] :or {if-header true}}]
  (try
    (if (fn? path)
      ;; if the path is the input function
      (if (:clojask-io (path))
        (let [io-func path
              read-func (fn [] (:data (io-func)))
              colNames (u/check-duplicate-col (if if-header (doall (first (read-func))) (generate-col (count (first (read-func))))))
              col-info (ColInfo. (doall (map keyword colNames)) {} {} {} {} {} {})
              row-info (RowInfo. [] [] [] [])
              stat (compute-stat path io-func)
              func (if if-header (fn [] (rest (read-func))) read-func)]
          (.init col-info colNames)
          (DataFrame. func 300 col-info row-info stat (atom (or (:output (io-func)) (fn [wtr msg] (.write wtr (str (str/join "," msg) "\n"))))) if-header))
        (let [headers (first (path))
            ;; headers (string/split (doall (first (path))) #",")
              colNames (u/check-duplicate-col (if if-header headers (generate-col (count headers))))
              col-info (ColInfo. (doall (map keyword colNames)) {} {} {} {} {} {})
              row-info (RowInfo. [] [] [] [])
              stat (compute-stat path)
              func (if if-header (fn [] (rest (path))) path)]
          (.init col-info colNames)
          (DataFrame. func 300 col-info row-info stat (atom (fn [wtr msg] (.write wtr (str (str/join "," msg) "\n")))) if-header)))
      ;; if the path is csv
      (let [io-func (fn [] (read-file path :stat true :output true))
            read-func (fn [] (:data (io-func)))
            ;; file (read-file path :stat true)
            ;; reader (io/reader path)
            ;; file (csv/read-csv reader)
            ;; data (:data file)
            colNames (u/check-duplicate-col (if if-header (doall (first (read-func))) (generate-col (count (first (read-func))))))
            col-info (ColInfo. (doall (map keyword colNames)) {} {} {} {} {} {})
            row-info (RowInfo. [] [] [] [])
            ;; stat (compute-stat path)
            stat (compute-stat path io-func)
            func (if if-header (fn [] (rest (read-func))) read-func)]
      ;; (type-detection file)
        ;; (.close reader)
        (.init col-info colNames)
      ;; 
      ;; type detection
      ;; 
        ;; (DataFrame. func 300 col-info row-info stat if-header)
        (DataFrame. func 300 col-info row-info stat (atom (or (:output (io-func)) (fn [wtr msg] (.write wtr (str (str/join "," msg) "\n"))))) if-header)
        ))
    (catch Exception e
      (do
        (throw (TypeException. "Error in initializing the dataframe." e))
        ;; (throw (OperationException. "no such file or directory"))
        nil))))

(defn filter
  [this cols predicate]
  (let [result (.filter this cols predicate)]
    (.errorPredetect this "invalid arguments passed to filter function")
    result))

(defn operate
  ([this operation colName]
  (let [result (.operate this operation colName)]
    (.errorPredetect this "this function cannot be appended into the current pipeline")
    result))
  ([this operation colName newCol]
  (let [result (.operate this operation colName newCol)]
    (.errorPredetect this "this function cannot be appended into the current pipeline")
    result)))

(defn group-by
  [this key]
  (let [result (.groupby this key)]
    (.errorPredetect this "invalid arguments passed to groupby function")
    result))

(defn aggregate
  [this func old-key & [new-key]]
  (let [old-key (if (coll? old-key)
                  old-key
                  [old-key])
        new-key (if (coll? new-key) ;; to do
                  new-key
                  (if (not= new-key nil)
                    [new-key]
                    (let [func-str (str func)
                          bgn-idx (+ (string/index-of func-str "$") 1)
                          end-idx (string/index-of func-str "@" bgn-idx)
                          col-func-str (subs func-str bgn-idx end-idx)]
                          (mapv (fn [_] (str col-func-str "(" _ ")")) old-key))
                    ))]
    (let [result (.aggregate this func old-key new-key)]
      (.errorPredetect this "invalid arguments passed to aggregate function")
      result)))

(defn sort
  [this list output-dir]
  (u/init-file output-dir nil)
  (.sort this list output-dir))

(defn set-type
  [this col type]
  (let [result (.setType this type col)]
    (.errorPredetect this "invalid arguments passed to set-type function")
  result))

(defn set-parser
  [this col parser]
  (let [result (.setParser this parser col)]
    (.errorPredetect this "invalid arguments passed to set-parser function")
    result))

(defn set-formatter
  [this col formatter]
  (let [result (.addFormatter this formatter col)]
    (.errorPredetect this "invalid arguments passed to set-formatter function")
    result))

;; (defn col-names
;;   [this]
;;   (let [result (.getColNames this)]
;;     (.errorPredetect this "invalid arguments passed to col-names function")
;;   result))

(defn select-col
  [this col-to-keep]
  (let [col-to-del (set/difference (set (.getColNames this)) (set col-to-keep))] 
    (.delCol this (vec col-to-del))))

(defn delete-col
  [this col-to-del]
  (let [result (.delCol this col-to-del)]
    (.errorPredetect this "invalid arguments passed to delete-col function")
  result))

(defn reorder-col
  [this new-col-order]
  (let [result (.reorderCol this new-col-order)]
    (.errorPredetect this "invalid arguments passed to reorder-col function")
  result))

(defn rename-col
  [this old-col new-col]
  (let [result (.renameCol this old-col new-col)]
    (.errorPredetect this "invalid arguments passed to rename-col function")
  result))

;; ============= Below is the definition for the joineddataframe ================

(definterface JDFIntf
  (checkInputPathClash [path] "check if paths clashes with dataframes a/b input path")
  (getColNames [] "get the names of all the columns")
  (printCol [output-path selected-col out] "print column names to output file")
  (setOutput [output])
  (getOutput [])
  (preview [] "preview the column names")
  (compute [^int num-worker ^String output-dir ^boolean exception ^boolean order select ifheader out]))

;; (defn joineddf-ctor
;;   [a b a-keys b-keys a-roll b-roll type limit prefix output-func]
;;   (JoinedDataFrame. a b a-keys b-keys a-roll b-roll type limit prefix output-func))

(defrecord JoinedDataFrame
           [^DataFrame a
            ^DataFrame b
            a-keys
            b-keys
            a-roll
            b-roll
            type
            limit
            prefix
            output-func]
  JDFIntf

  (checkInputPathClash
    [this path]
    (.checkInputPathClash a path)
    (.checkInputPathClash b path))

  (getColNames
    [this]
    (let [a-col-prefix (first prefix)
          b-col-prefix (last prefix)
          a-col-set (.getColNames a)
          b-col-set (.getColNames b)
          a-col-header (mapv #(str a-col-prefix "_" %) a-col-set)
          b-col-header (mapv #(str b-col-prefix "_" %) b-col-set)]
      (concat a-col-header b-col-header)))

  (setOutput
    [this output]
    (reset! output-func output))

  (getOutput
    [this]
    (deref output-func))

  (printCol
    ;; print column names, called by compute
    [this output-path selected-index out]
    (let [col-set (if (= selected-index [nil]) (.getColNames this) (mapv (vec (.getColNames this)) selected-index))]
      (let [wrtr (if output-path (io/writer output-path) nil)]
        ((or out (.getOutput this)) wrtr [col-set])
        (if output-path (.close wrtr)))))

  (preview
    [this]
    (.getColNames this))

  (compute
    [this ^int num-worker ^String output-dir ^boolean exception ^boolean order select ifheader out]
    (let [select (if (coll? select) select [select])
          select (if (= select [nil])
                   (vec (take (+ (count (.getKeyIndex (.col-info a))) (count (.getKeyIndex (.col-info b)))) (iterate inc 0)))
                   (mapv (fn [key] (.indexOf (.getColNames this) key)) select))
          a-index (vec (apply sorted-set (remove (fn [num] (>= num (count (.getKeyIndex (.col-info a))))) select)))
          ;; a-write 
          b-index (mapv #(- % (count (.getKeyIndex (.col-info a)))) (apply sorted-set (remove (fn [num] (< num (count (.getKeyIndex (.col-info a))))) select)))
          b-index (if b-roll (vec (apply sorted-set (conj b-index b-roll))) b-index)
          b-roll (if b-roll (count (remove #(>= % b-roll) b-index)) nil)
          ;; b-write
          a-format (set/rename-keys (.getFormatter (.col-info a)) (zipmap a-index (iterate inc 0)))
          b-format (set/rename-keys (.getFormatter (.col-info b)) (zipmap b-index (iterate inc 0)))
          write-index (mapv (fn [num] (count (remove #(>= % num) (concat a-index (mapv #(+ % (count (.getKeyIndex (.col-info a)))) b-index))))) select)
          ;; test (println a-index b-index b-format write-index b-roll)
          ]
      ;; (u/init-file output-dir)
      ;; print column names
      (if (= ifheader nil) (.printCol this output-dir select out))
      (if (not= type 3)
        (do
          (start-onyx-groupby num-worker 10 b "./.clojask/join/b/" b-keys b-index exception) ;; todo
          (start-onyx-join num-worker 10 a b output-dir exception a-keys b-keys a-roll b-roll type limit a-index (vec (take (count b-index) (iterate inc 0))) b-format write-index out))
        (do
          (start-onyx-groupby num-worker 10 a "./.clojask/join/a/" a-keys a-index exception)
          (start-onyx-groupby num-worker 10 b "./.clojask/join/b/" b-keys b-index exception)
          (start-onyx-outer num-worker 10 a b output-dir exception a-index b-index a-format b-format write-index))))))

(defn inner-join
  [a b a-keys b-keys & {:keys [col-prefix] :or {col-prefix ["1" "2"]}}]
  (let [a-keys (u/proc-groupby-key a-keys)
        b-keys (u/proc-groupby-key b-keys)
        a-keys (mapv (fn [_] [(nth _ 0) (get (.getKeyIndex (.col-info a)) (nth _ 1))]) a-keys)
        b-keys (mapv (fn [_] [(nth _ 0) (get (.getKeyIndex (.col-info b)) (nth _ 1))]) b-keys)]
    (cond (not (and (= (type a) clojask.dataframe.DataFrame) (= (type b) clojask.dataframe.DataFrame))) 
      (throw (TypeException. "First two arguments should be Clojask dataframes.")))
    (cond (or (not= (.getAggreFunc (:row-info a)) []) (not= (.getGroupbyKeys (:row-info a)) []) (not= (.getAggreFunc (:row-info b)) []) (not= (.getGroupbyKeys (:row-info b)) []))
          (throw (TypeException. "Cannot join on a dataframe that has been grouped by or aggregated. Try to first compute, then use the new one to join.")))
    (cond (not (= (count a-keys) (count b-keys))) 
      (throw (TypeException. "The length of left keys and right keys should be equal.")))
    (cond (not (and (u/are-in a-keys a) (u/are-in b-keys b))) 
      (throw (TypeException. "Input includes non-existent column name(s).")))
    (let [size-a (.getSize (:stat a))
          size-b (.getSize (:stat b))]
      (if (>= size-a size-b)
        (JoinedDataFrame. a b a-keys b-keys nil nil 1 nil col-prefix (atom (.getOutput a)))
        (JoinedDataFrame. b a b-keys a-keys nil nil 1 nil [(nth col-prefix 1) (nth col-prefix 0)] (atom (.getOutput a)))))))

(defn left-join
  [a b a-keys b-keys & {:keys [col-prefix] :or {col-prefix ["1" "2"]}}]
  (let [a-keys (u/proc-groupby-key a-keys)
        b-keys (u/proc-groupby-key b-keys)
        a-keys (mapv (fn [_] [(nth _ 0) (get (.getKeyIndex (.col-info a)) (nth _ 1))]) a-keys)
        b-keys (mapv (fn [_] [(nth _ 0) (get (.getKeyIndex (.col-info b)) (nth _ 1))]) b-keys)]
    (cond (not (and (= (type a) clojask.dataframe.DataFrame) (= (type b) clojask.dataframe.DataFrame))) 
      (throw (TypeException. "First two arguments should be Clojask dataframes.")))
    (cond (or (not= (.getAggreFunc (:row-info a)) []) (not= (.getGroupbyKeys (:row-info a)) []) (not= (.getAggreFunc (:row-info b)) []) (not= (.getGroupbyKeys (:row-info b)) []))
          (throw (TypeException. "Cannot join on a dataframe that has been grouped by or aggregated. Try to first compute, then use the new one to join.")))
    (cond (not (= (count a-keys) (count b-keys))) 
      (throw (TypeException. "The length of left keys and right keys should be equal.")))
    (cond (not (= (count col-prefix) 2)) 
      (throw (TypeException. "The length of col-prefix should be equal to 2.")))
    (cond (not (and (u/are-in a-keys a) (u/are-in b-keys b))) 
      (throw (TypeException. "Input includes non-existent column name(s).")))
    (JoinedDataFrame. a b a-keys b-keys nil nil 2 nil col-prefix (atom (.getOutput a)))))

(defn right-join
  [a b a-keys b-keys & {:keys [col-prefix] :or {col-prefix ["1" "2"]}}]
  (let [a-keys (u/proc-groupby-key a-keys)
        b-keys (u/proc-groupby-key b-keys)
        a-keys (mapv (fn [_] [(nth _ 0) (get (.getKeyIndex (.col-info a)) (nth _ 1))]) a-keys)
        b-keys (mapv (fn [_] [(nth _ 0) (get (.getKeyIndex (.col-info b)) (nth _ 1))]) b-keys)]
    (cond (not (and (= (type a) clojask.dataframe.DataFrame) (= (type b) clojask.dataframe.DataFrame))) 
      (throw (TypeException. "First two arguments should be Clojask dataframes.")))
    (cond (or (not= (.getAggreFunc (:row-info a)) []) (not= (.getGroupbyKeys (:row-info a)) []) (not= (.getAggreFunc (:row-info b)) []) (not= (.getGroupbyKeys (:row-info b)) []))
          (throw (TypeException. "Cannot join on a dataframe that has been grouped by or aggregated. Try to first compute, then use the new one to join.")))
    (cond (not (= (count a-keys) (count b-keys))) 
      (throw (TypeException. "The length of left keys and right keys should be equal.")))
    (cond (not (and (u/are-in a-keys a) (u/are-in b-keys b))) 
      (throw (TypeException. "Input includes non-existent column name(s).")))
    (JoinedDataFrame. b a b-keys a-keys nil nil 2 nil [(nth col-prefix 1) (nth col-prefix 0)] (atom (.getOutput a)))))

(defn outer-join
  [a b a-keys b-keys & {:keys [col-prefix] :or {col-prefix ["1" "2"]}}]
  (let [a-keys (u/proc-groupby-key a-keys)
        b-keys (u/proc-groupby-key b-keys)
        a-keys (mapv (fn [_] [(nth _ 0) (get (.getKeyIndex (.col-info a)) (nth _ 1))]) a-keys)
        b-keys (mapv (fn [_] [(nth _ 0) (get (.getKeyIndex (.col-info b)) (nth _ 1))]) b-keys)]
    (cond (not (and (= (type a) clojask.dataframe.DataFrame) (= (type b) clojask.dataframe.DataFrame)))
          (throw (TypeException. "First two arguments should be Clojask dataframes.")))
    (cond (or (not= (.getAggreFunc (:row-info a)) []) (not= (.getGroupbyKeys (:row-info a)) []) (not= (.getAggreFunc (:row-info b)) []) (not= (.getGroupbyKeys (:row-info b)) []))
          (throw (TypeException. "Cannot join on a dataframe that has been grouped by or aggregated. Try to first compute, then use the new one to join.")))
    (cond (not (= (count a-keys) (count b-keys)))
          (throw (TypeException. "The length of left keys and right keys should be equal.")))
    (cond (not (and (u/are-in a-keys a) (u/are-in b-keys b)))
          (throw (TypeException. "Input includes non-existent column name(s).")))
    (let [size-a (.getSize (:stat a))
          size-b (.getSize (:stat b))]
      (if (<= size-a size-b)
        (JoinedDataFrame. a b a-keys b-keys nil nil 3 nil col-prefix (atom (.getOutput a)))
        (JoinedDataFrame. b a b-keys a-keys nil nil 3 nil [(nth col-prefix 1) (nth col-prefix 0)] (atom (.getOutput a)))))))

(defn rolling-join-forward
  [a b a-keys b-keys a-roll b-roll & {:keys [col-prefix limit] :or {col-prefix ["1" "2"] limit nil}}]
  (let [a-keys (u/proc-groupby-key a-keys)
        b-keys (u/proc-groupby-key b-keys)
        a-keys (mapv (fn [_] [(nth _ 0) (get (.getKeyIndex (.col-info a)) (nth _ 1))]) a-keys)
        b-keys (mapv (fn [_] [(nth _ 0) (get (.getKeyIndex (.col-info b)) (nth _ 1))]) b-keys)]
    (cond (not (and (= (type a-roll) java.lang.String) (= (type b-roll) java.lang.String)))
      (throw (TypeException. "Rolling keys should be strings")))
    (cond (not (and (= (type a) clojask.dataframe.DataFrame) (= (type b) clojask.dataframe.DataFrame))) 
      (throw (TypeException. "First two arguments should be Clojask dataframes.")))
    (cond (not (= (count a-keys) (count b-keys))) 
      (throw (TypeException. "The length of left keys and right keys should be equal.")))
    (cond (not (and (u/are-in a-keys a) (u/are-in b-keys b))) 
      (throw (TypeException. "Input includes non-existent column name(s).")))
    (cond (or (not= (.getAggreFunc(:row-info a)) []) (not= (.getGroupbyKeys (:row-info a)) []) (not= (.getAggreFunc (:row-info b)) []) (not= (.getGroupbyKeys (:row-info b)) []))
          (throw (TypeException. "Cannot join on a dataframe that has been grouped by or aggregated. Try to first compute, then use the new one to join.")))
    (let [[a-roll b-roll] [(get (.getKeyIndex (:col-info a)) a-roll) (get (.getKeyIndex (:col-info b)) b-roll)]]
      (do
        (cond (not (and (not= a-roll nil) (not= b-roll nil)))
          (throw (TypeException. "Rolling keys include non-existent column name(s).")))
        (JoinedDataFrame. a b a-keys b-keys a-roll b-roll 4 limit col-prefix (atom (.getOutput a)))))))


;; all of the code is the same as above except for the last line
(defn rolling-join-backward
  [a b a-keys b-keys a-roll b-roll & {:keys [col-prefix limit] :or {col-prefix ["1" "2"] limit nil}}]
  (let [a-keys (u/proc-groupby-key a-keys)
        b-keys (u/proc-groupby-key b-keys)
        a-keys (mapv (fn [_] [(nth _ 0) (get (.getKeyIndex (.col-info a)) (nth _ 1))]) a-keys)
        b-keys (mapv (fn [_] [(nth _ 0) (get (.getKeyIndex (.col-info b)) (nth _ 1))]) b-keys)]
    (cond (not (and (= (type a-roll) java.lang.String) (= (type b-roll) java.lang.String)))
          (throw (TypeException. "Rolling keys should be strings")))
    (cond (not (and (= (type a) clojask.dataframe.DataFrame) (= (type b) clojask.dataframe.DataFrame)))
          (throw (TypeException. "First two arguments should be Clojask dataframes.")))
    (cond (or (not= (.getAggreFunc (:row-info a)) []) (not= (.getGroupbyKeys (:row-info a)) []) (not= (.getAggreFunc (:row-info b)) []) (not= (.getGroupbyKeys (:row-info b)) []))
          (throw (TypeException. "Cannot join on a dataframe that has been grouped by or aggregated. Try to first compute, then use the new one to join.")))
    (cond (not (= (count a-keys) (count b-keys)))
          (throw (TypeException. "The length of left keys and right keys should be equal.")))
    (cond (not (and (u/are-in a-keys a) (u/are-in b-keys b)))
          (throw (TypeException. "Input includes non-existent column name(s).")))
    (let [[a-roll b-roll] [(get (.getKeyIndex (:col-info a)) a-roll) (get (.getKeyIndex (:col-info b)) b-roll)]]
      (do
        (cond (not (and (not= a-roll nil) (not= b-roll nil)))
              (throw (TypeException. "Rolling keys include non-existent column name(s).")))
        (JoinedDataFrame. a b a-keys b-keys a-roll b-roll 5 limit col-prefix (atom (.getOutput a)))))))

(defn compute
  [this num-worker output-dir & {:keys [exception order output select exclude melt header] :or {exception false order false output nil select nil exclude nil melt vector header nil}}]
  (assert (or (nil? select) (nil? exclude)) "Can only specify either of select or exclude")
  ;; check if output-dir clashes with input file path
  ;; (.checkInputPathClash this output-dir)
  ;; initialise file
  (u/init-file output-dir header)
  ;; check which type of dataframe this is
  (let [exclude (if (coll? exclude) exclude [exclude])
        select (if select select (if (not= [nil] exclude) (doall (remove (fn [item] (.contains exclude item)) (.getColNames this))) nil))
        ret (atom (transient []))
        output-func (if output-dir 
                      (or output (.getOutput this)) 
                      (fn [wtr seq] (doseq [row seq] (reset! ret (conj! (deref ret) row)))))
        output-dir (or output-dir ".clojask/tmp.csv")]
    (assert (not= select []) "Must select at least 1 column")
    (assert (or (= melt vector) (and (= (type this) clojask.dataframe.DataFrame) (= (.getGroupbyKeys (:row-info this)) []) (= (.getAggreFunc (:row-info this)) []))) "melt is not applicable to this dataframe")
    ;; (if output (.setOutput this output))
    (if (= (type this) clojask.dataframe.DataFrame)
      (if (and (= (.getGroupbyKeys (:row-info this)) []) (= (.getAggreFunc (:row-info this)) []))
        (do ;; simple compute
          (.compute this num-worker output-dir exception order select melt header output-func)
          ) ;; return output dataframe
        (if (not= (.getGroupbyKeys (:row-info this)) [])
          (do ;; groupby-aggre
            (.computeGroupAggre this num-worker output-dir exception select header output-func))
          (do ;; aggre
            (.computeAggre this num-worker output-dir exception select header output-func))))
      (if (= (type this) clojask.dataframe.JoinedDataFrame)
        (do ;; join
          (.compute this num-worker output-dir exception order select header output-func)
          )
        (throw (TypeException. "Must compute on a clojask dataframe or joined dataframe"))))
    (if (not= output-dir ".clojask/tmp.csv")
      (dataframe output-dir :have-col true)
      (persistent! (deref ret)))))

(defn get-col-names
  "Get the names for the columns in sequence"
  [this]
  (.getColNames this))

(defn print-df
  [dataframe & [sample-size return-size]]
  (assert (or (= (type dataframe) clojask.dataframe.DataFrame) (= (type dataframe) clojask.dataframe.JoinedDataFrame)) "Please input a clojask dataframe.")
  (if (= (type dataframe) clojask.dataframe.DataFrame)
    (let [data (.preview dataframe (or sample-size 1000) (inc (or return-size 10)) false)
          tmp (first data)
          key (keys tmp)
          types (zipmap key (map u/get-type-string-vec (map #(for [row data] (get row %)) key)))
          omit (zipmap (keys tmp) (repeat "..."))
          data (vec (conj (apply list data) types))
          data (if (= (count data) (inc (or return-size 11)))
                 (conj (vec (take (or return-size 10) data)) omit)
                 data)
          header (.getColNames dataframe)]
      (pprint/print-table header data))
    (do
      (println (str (str/join "," (.preview dataframe))))
      (println "The content of joined dataframe is not available."))))
