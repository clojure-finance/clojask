(ns clojask.DataFrame
  (:require [clojask.ColInfo :refer [->ColInfo]]
            [clojask.RowInfo :refer [->RowInfo]]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojask.utils :as u]
            ;; [clojask.groupby :refer [internal-aggregate aggre-min]]
            [clojask.onyx-comps :refer [start-onyx start-onyx-groupby start-onyx-join]]
            [clojask.sort :as sort]
            ;; [clojask.join :as join]
            [aggregate.aggre-onyx-comps :refer [start-onyx-aggre]]
            [clojure.string :as str]
            [clojask.preview :as preview]
            [clojure.pprint :as pprint]
            )
  (:import [clojask.ColInfo ColInfo]
           [clojask.RowInfo RowInfo]))
"The clojask lazy dataframe"

(import '[com.clojask.exception Clojask_TypeException]
        '[com.clojask.exception Clojask_OperationException])

(definterface DFIntf
  (compute [^int num-worker ^String output-dir ^boolean exception ^boolean order])
  (operate [operation colName] "operate an operation to column and replace in place")
  (operate [operation colName newCol] "operate an operation to column and add the result as new column")
  (setType [type colName] "types supported: int double string date")
  (setParser [parser col] "add the parser for a col which acts like setType")
  (colDesc [])
  (colTypes [])
  (printCol [output-path] "print column names to output file")
  (printAggreCol [output-path] "print column names to output file for join & aggregate")
  (reorderCol [new-col-order] "reorder columns in the dataframe")
  (renameCol [new-col-names] "reorder columns in the dataframe")
  (groupby [a] "group the dataframe by the key(s)")
  (aggregate [a c b] "aggregate the group-by result by the function")
  (head [n])
  (filter [cols predicate])
  (computeAggre [^int num-worker ^String output-dir ^boolean exception])
  (sort [a b] "sort the dataframe based on columns")
  (addFormatter [a b] "format the column as the last step of the computation")
  (preview [sample-size output-size format] "quickly return a vector of maps about the resultant dataframe")
  (final [] "prepare the dataframe for computation")
  )

;; each dataframe can have a delayed object
(defrecord DataFrame
           [^String path
            ^Integer batch-size
            ^ColInfo col-info
            ^RowInfo row-info
            ^Boolean have-col]
  DFIntf
  (operate ;; has assert
    [this operation colName]
    (if (nil? (.operate col-info operation colName))
    this ; "success"
    (throw (Clojask_OperationException. "operate"))))
  (operate
    [this operation colNames newCol]
    (assert (= java.lang.String (type newCol)) "new column should be a string")
    (if (nil? (.operate col-info operation colNames newCol))
      this ; "success"
      (throw (Clojask_OperationException. "operate"))))
  (groupby
    [this key]
    (let [keys (if (coll? key)
                 key
                 [key])]
      (assert (= 0 (count (u/are-in keys this))) "input is not existing column names")
      (let [keys (mapv (fn [_] (get (.getKeyIndex col-info) _)) keys)]
        (if (nil? (.groupby row-info keys))
          this
          (throw (Clojask_OperationException. "groupby"))
          ))))
  (aggregate
    [this func old-key new-key]
    (assert (= 0 (count (u/are-in old-key this))) "input is not existing column names")
    (assert (= 0 (count (u/are-out new-key this))) "new keys should not be existing column names")
    (let [old-key (mapv (fn [_] (get (.getKeyIndex col-info) _)) old-key)]
      (if (nil? (.aggregate row-info func old-key new-key))
        this
        (throw (Clojask_OperationException. "aggregate"))
        )))
  (filter
    [this cols predicate]
    (let [cols (if (coll? cols)
                 cols
                 (vector cols))
          indices (map (fn [_] (get (.getKeyIndex (:col-info this)) _)) cols)]
      (assert (u/are-in cols this) "input is not existing column names")
      (if (nil? (.filter row-info indices predicate))
        this
        (throw (Clojask_OperationException. "filter"))
        )))
  (colDesc
    [this]
    (.getDesc col-info))
  (colTypes
    [this]
    (.getType col-info))
  (printCol
    [this output-path]
    (assert (= java.lang.String (type output-path)) "output path should be a string")
    (with-open [wrtr (io/writer output-path)]
      (.write wrtr (str (str/join "," (map last (.getIndexKey (.col-info this)))) "\n")))
    )
  (printAggreCol
    [this output-path]
    (assert (= java.lang.String (type output-path)) "output path should be a string")
    (let [groupby-key-index (.getGroupbyKeys (:row-info this))
          groupby-keys (vec (map (.getIndexKey (.col-info this)) groupby-key-index))
          aggre-new-keys (.getAggreNewKeys (:row-info this))]
        (with-open [wrtr (io/writer output-path)]
          (.write wrtr (str (str/join "," (concat groupby-keys aggre-new-keys)) "\n")))
      ))
  (reorderCol
    [this new-col-order]
    (cond (not (= (set (.getKeys (.col-info this))) (set new-col-order))) 
      (throw (Clojask_TypeException. "set of input in reorder-col contains column that do not exist in dataframe")))
    (.setColInfo (.col-info this) new-col-order)
    (.setRowInfo (.row-info this) (.getDesc (.col-info this)) new-col-order))
  (renameCol
    [this new-col-names]
    (cond (not (= (count (.getKeys (.col-info this))) (count new-col-names)))
      (throw (Clojask_TypeException. "number of new column names not equal to number of existing columns")))
    (.renameColInfo (.col-info this) new-col-names))
  (head
    [this n]
    (assert (integer? n) "argument passed to head should be an integer")
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
    (assert (u/is-in colName this) "input is not existing column name")
    (if (nil? (.setType col-info parser colName))
      this
      (throw (Clojask_OperationException. "setParser"))))
  (addFormatter
    [this format col]
    (assert (u/is-in col this) "input is not existing column name")
    (.setFormatter col-info format col))
  (final
    [this]
    (doseq [tmp (.getFormatter (:col-info this))]
      (.operate this (nth tmp 1) (get (.getIndexKey col-info) (nth tmp 0)))))
    ;; currently put read file here

  (preview
   [this sample-size return-size format]
   (assert (and (integer? sample-size) (integer? return-size)) "arguments passed to preview should be integers")
   (preview/preview this sample-size return-size format)
   )
  (compute
    [this ^int num-worker ^String output-dir ^boolean exception ^boolean order]
    ;(assert (= java.lang.String (type output-dir)) "output path should be a string")
    (if (<= num-worker 8)
      (try
        (.final this)
        (.printCol this output-dir) ;; print column names to output-dir
        (let [res (start-onyx num-worker batch-size this output-dir exception order)]
          (if (= res "success")
            "success"
            "failed"))
        (catch Exception e e))
        (throw (Clojask_OperationException. "Max number of work nodes is 8."))))
  (computeAggre
    [this ^int num-worker ^String output-dir ^boolean exception]
    (assert (= java.lang.String (type output-dir)) "output dir should be a string")
    (if (<= num-worker 8)
      (try
        (let [res (start-onyx-groupby num-worker batch-size this "_clojask/grouped/" (.getGroupbyKeys (:row-info this)) exception)]
          (.printAggreCol this output-dir) ;; print column names to output-dir
          (if (= res "success")
          ;;  (if (= "success" (start-onyx-aggre num-worker batch-size this output-dir (.getGroupbyKeys (:row-info this)) exception))
            (if
            ;;  (internal-aggregate (.getAggreFunc (:row-info this)) output-dir (.getKeyIndex col-info) (.getGroupbyKeys (:row-info this)) (.getAggreOldKeys (:row-info this)) (.getAggreNewKeys (:row-info this)))
             (start-onyx-aggre num-worker batch-size this output-dir exception)
              "success"
              (throw (Clojask_OperationException. "start-onyx-aggre")))
            (throw (Clojask_OperationException. "start-onyx-groupby"))))
        (catch Exception e e))
        (throw (Clojask_OperationException. "Max number of work nodes is 8."))))
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
              (let [tmp (compare (u/get-key a (.getType col-info) (.getKeyIndex col-info) _) (u/get-key b (.getType col-info) (.getKeyIndex col-info) _))]
                (if (not= tmp 0)
                  (sign tmp)
                  (recur res false nil)))
              (if (= _ "+")
                (recur res true +)
                (recur res true -)))))))
    (sort/use-external-sort path output-dir clojask-compare)))

(defn generate-col
  "Generate column names if there are none"
  [col-count]
  (vec (map #(str "Col_" %) (range 1 (+ col-count 1)))))

(defn dataframe
  [path & {:keys [have-col] :or {have-col true}}]
  (try
    (let [reader (io/reader path)
          file (csv/read-csv reader)
          colNames (u/check-duplicate-col (if have-col (doall (first file)) (generate-col (count (first file)))))
          col-info (ColInfo. (doall (map keyword colNames)) {} {} {} {} {})
          row-info (RowInfo. [] [] [] [])]
      ;; (type-detection file)
      (.close reader)
      (.init col-info colNames)
      ;; 
      ;; type detection
      ;; 
      (DataFrame. path 300 col-info row-info have-col))
    (catch Exception e
      (do
        ;; (println "No such file or directory")
        (throw e)
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
  [this num-worker output-dir & {:keys [exception order] :or {exception false order true}}]
  (u/init-file output-dir)
  (if (= (.getAggreFunc (:row-info this)) [])
    (.compute this num-worker output-dir exception order)
    (.computeAggre this num-worker output-dir exception)))

(defn group-by
  [this key]
  (.groupby this key))

(defn aggregate
  [this func old-key & [new-key]]
  (let [old-key (if (coll? old-key)
                  old-key
                  [old-key])
        new-key (if (coll? new-key)   ;; to do
                  new-key
                  (if (not= new-key nil)
                    [new-key]
                    (mapv (fn [_] (str func "(" _ ")")) old-key)))]
   (.aggregate this func old-key new-key)))

(defn sort
  [this list output-dir]
  (u/init-file output-dir)
  (.sort this list output-dir))

(defn set-type
  [this col type]
  (.setType this type col))

(defn set-parser
  [this col parser]
  (.setParser this parser col))

(defn preview
  [dataframe sample-size return-size & {:keys [format] :or {format false}}]
  (.preview dataframe sample-size return-size format))

(defn print-df
  [dataframe & [sample-size return-size]]
  (let [data (.preview dataframe (or sample-size 1000) (or return-size 10) false)
        tmp (first data)
        types (zipmap (keys tmp) (map u/get-type-string (vals tmp)))
        data (conj (apply list data) types)]
    (pprint/print-table data)))

(defn reorder-col
  [this new-col-order]
  (.reorderCol this new-col-order))

(defn rename-col
  [this new-col-names]
  (.renameCol this new-col-names))

(defn inner-join
  [a b a-keys b-keys num-worker dist & {:keys [exception] :or {exception false}}]
  (assert (and (= (type b) clojask.DataFrame.DataFrame) (= (type a) clojask.DataFrame.DataFrame)) "First two arguments should be clojask dataframes.")
  (assert (= (count a-keys) (count b-keys)) "The length of left keys and right keys should be equal.")
  (u/init-file dist)
  ;; first group b by keys
  ;; (start-onyx-groupby num-worker batch-size a "./_clojask/join/a/" a-keys false)
  (let [a-keys (vec (map (fn [_] (get (.getKeyIndex (.col-info a)) _)) a-keys))
        b-keys (vec (map (fn [_] (get (.getKeyIndex (.col-info b)) _)) b-keys))]
    (start-onyx-groupby num-worker 10 b "./_clojask/join/b/" b-keys exception)
    (start-onyx-join num-worker 10 a b dist exception a-keys b-keys nil nil 1))
  )

(defn left-join
  [a b a-keys b-keys num-worker dist & {:keys [exception] :or {exception false}}]
  ;(assert (and (= (type b) clojask.DataFrame.DataFrame) (= (type a) clojask.DataFrame.DataFrame)) "First two arguments should be clojask dataframes.")
  (assert (= (count a-keys) (count b-keys)) "The length of left keys and right keys should be equal.")
  (u/init-file dist)
  ;; first group b by keys
  ;; (start-onyx-groupby num-worker batch-size a "./_clojask/join/a/" a-keys false)
  (let [a-keys (vec (map (fn [_] (get (.getKeyIndex (.col-info a)) _)) a-keys))
        b-keys (vec (map (fn [_] (get (.getKeyIndex (.col-info b)) _)) b-keys))]
    (start-onyx-groupby num-worker 10 b "./_clojask/join/b/" b-keys exception)
    (start-onyx-join num-worker 10 a b dist exception a-keys b-keys nil nil 2)))

(defn right-join
  [a b a-keys b-keys num-worker dist & {:keys [exception] :or {exception false}}]
  (assert (and (= (type b) clojask.DataFrame.DataFrame) (= (type a) clojask.DataFrame.DataFrame)) "First two arguments should be clojask dataframes.")
  (assert (= (count a-keys) (count b-keys)) "The length of left keys and right keys should be equal.")
  (u/init-file dist)
  ;; first group b by keys
  ;; (start-onyx-groupby num-worker batch-size a "./_clojask/join/a/" a-keys false)
  (let [a-keys (vec (map (fn [_] (get (.getKeyIndex (.col-info a)) _)) a-keys))
        b-keys (vec (map (fn [_] (get (.getKeyIndex (.col-info b)) _)) b-keys))]
    (start-onyx-groupby num-worker 10 a "./_clojask/join/b/" a-keys exception)
    (start-onyx-join num-worker 10 b a dist exception b-keys a-keys nil nil 2)))

(defn rolling-join-forward
  [a b a-keys b-keys a-roll b-roll num-worker dist & {:keys [exception] :or {exception false}}]
  (assert (= (type a-roll) java.lang.String))
  (assert (= (type b-roll) java.lang.String))
  (assert (= (count a-keys) (count b-keys)) "The length of left keys and right keys should be equal.")
  (let [[a-roll b-roll] [(get (.getKeyIndex (:col-info a)) a-roll) (get (.getKeyIndex (:col-info b)) b-roll)]]
    (do
      (assert (and (not= a-roll nil) (not= b-roll nil)) "rolling key should be existing header")
      (u/init-file dist)
  ;; (join/internal-rolling-join-forward a-keys b-keys a-roll b-roll)
      (let [a-keys (vec (map (fn [_] (get (.getKeyIndex (.col-info a)) _)) a-keys))
            b-keys (vec (map (fn [_] (get (.getKeyIndex (.col-info b)) _)) b-keys))]
        (start-onyx-groupby num-worker 10 b "./_clojask/join/b/" b-keys exception)
        (start-onyx-join num-worker 10 a b dist exception a-keys b-keys a-roll b-roll 4)))
    )
  ;; (start-onyx-groupby num-worker 10 a "./_clojask/join/a/" a-keys exception)
  
  )
