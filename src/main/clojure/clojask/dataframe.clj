(ns clojask.dataframe
  (:require clojask-io.core
            [clojask-io.input :refer [read-file]]
            [clojask-io.output :as output]
            [clojask.classes.DataStat :refer [compute-stat]]
            [clojask.classes.ColInfo]
            [clojask.classes.RowInfo]
            [clojask.classes.DataFrame]
            [clojask.classes.JoinedDataFrame]
            [clojask.utils :as u]
            [clojure.pprint :as pprint]
            [clojure.set :as set]
            [clojure.string :as str]
            [clojure.java.io :as io]
            [taoensso.timbre :as timbre])
  (:import [clojask.classes.ColInfo ColInfo]
           [clojask.classes.RowInfo RowInfo]
           [clojask.classes.DataFrame DataFrame]
           [clojask.classes.JoinedDataFrame JoinedDataFrame]
           [com.clojask.exception TypeException OperationException])
  (:refer-clojure :exclude [filter group-by sort]))

;; debug APIs

(timbre/merge-config! {:min-level :warn})

(defn enable-debug
  []
  (reset! clojask.classes.DataFrame/debug true)
  (timbre/merge-config! {:min-level :debug})
  (println "Debug mode is on."))

(defn disable-debug
  []
  (reset! clojask.classes.DataFrame/debug false)
  (timbre/merge-config! {:min-level :warn})
  (println "Debug mode is off."))

;; public APIs of the dataframe

(defn preview
  [dataframe sample-size return-size & {:keys [format] :or {format false}}]
  (.preview dataframe sample-size return-size format))

(defn get-col-names
  "Get the names for the columns in sequence"
  [this]
  (vec (.getColNames this)))

(defn print-df
  [dataframe & [sample-size return-size]]
  (assert (or (= (type dataframe) clojask.classes.DataFrame.DataFrame) (= (type dataframe) clojask.classes.JoinedDataFrame.JoinedDataFrame)) "Please input a clojask dataframe.")
  (if (= (type dataframe) clojask.classes.DataFrame.DataFrame)
    (let [data (.preview dataframe (or sample-size 1000) (+ 1 (or return-size 10)) false)
          tmp (first data)
          key (keys tmp)
          types (zipmap key (map u/get-type-string-vec (map #(for [row data] (get row %)) key)))
          omit (zipmap key (repeat "..."))
          ;; preview fetched one row more than is shown, to know whether to print "..."
          data (if (> (count data) (or return-size 10))
                 (conj (vec (take (or return-size 10) data)) omit)
                 (vec data))
          data (into [types] data)
          header (.getColNames dataframe)]
      (pprint/print-table header data))
    (do
      (let [data (.preview dataframe (or sample-size 1000) (inc (or return-size 10)) false)
            key (keys (first data))
            types (zipmap key (map u/get-type-string-vec (map #(for [row data] (get row %)) key)))
            omit (zipmap key (repeat "..."))
            header (.getColNames dataframe)
            types (conj [types] omit)
            ]
        (pprint/print-table header types))
      (println "The content of joined dataframe is not available for preview."))))

(defn- generate-col
  "Generate column names if there are none"
  [col-count]
  (vec (map #(str "Col_" %) (range 1 (+ col-count 1)))))

(defn dataframe
  [path & {:keys [if-header] :or {if-header true}}]
  (try
    (if (fn? path)
      ;; probe is realized once: header row, :path, :output, then closed
      (let [probe (path)]
        ;; if the input is the clojask-io.input
        (if (:clojask-io probe)
          (let [io-func path
                read-func (fn [] (:data (io-func)))
                colNames (u/check-duplicate-col (if if-header (doall (first (:data probe))) (generate-col (count (first (:data probe))))))
                col-info (ColInfo. (doall (map keyword colNames)) {} {} {} {} {} {} {})
                row-info (RowInfo. [] [] [] [] {})
                stat (compute-stat path (fn [] probe))
                ;; arity 1 exposes {:data ... :close ...} so partial consumers
                ;; (preview / errorPredetect) can close the reader they abandon
                func (if if-header
                       (fn ([] (rest (read-func)))
                         ([_] (let [res (io-func)] {:data (rest (:data res)) :close (:close res)})))
                       (fn ([] (read-func))
                         ([_] (let [res (io-func)] {:data (:data res) :close (:close res)}))))
                output-func (or (:output probe) (fn [wtr rows] (doseq [msg rows] (.write wtr (str (str/join "," msg) "\n")))))]
            (.init col-info colNames)
            (when-let [close (:close probe)] (close))
            (DataFrame. (:path probe) func 300 col-info row-info stat (atom output-func) if-header))
          ;; if the is the lazy seq function
          (let [headers (first probe)
                colNames (u/check-duplicate-col (if if-header headers (generate-col (count headers))))
                col-info (ColInfo. (doall (map keyword colNames)) {} {} {} {} {} {} {})
                row-info (RowInfo. [] [] [] [] {})
                stat (compute-stat path)
                func (if if-header
                       (fn ([] (rest (path)))
                         ([_] {:data (rest (path))}))
                       (fn ([] (path))
                         ([_] {:data (path)})))]
            (.init col-info colNames)
            (DataFrame. nil func 300 col-info row-info stat (atom (fn [wtr rows] (doseq [msg rows] (.write wtr (str (str/join "," msg) "\n"))))) if-header))))
      ;; if the input is the path string
      (let [io-func (fn [] (read-file path :stat true :output true))
            probe (io-func)
            read-func (fn [] (:data (io-func)))
            colNames (u/check-duplicate-col (if if-header (doall (first (:data probe))) (generate-col (count (first (:data probe))))))
            col-info (ColInfo. (doall (map keyword colNames)) {} {} {} {} {} {} {})
            row-info (RowInfo. [] [] [] [] {})
            stat (compute-stat path (fn [] probe))
            func (if if-header
                   (fn ([] (rest (read-func)))
                     ([_] (let [res (io-func)] {:data (rest (:data res)) :close (:close res)})))
                   (fn ([] (read-func))
                     ([_] (let [res (io-func)] {:data (:data res) :close (:close res)}))))
            output-func (or (:output probe) (fn [wtr rows] (doseq [msg rows] (.write wtr (str (str/join "," msg) "\n")))))]
        (.init col-info colNames)
        (when-let [close (:close probe)] (close))
        (DataFrame. path func 300 col-info row-info stat (atom output-func) if-header)))
    (catch Exception e
      (do
        (throw (TypeException. "Error in initializing the dataframe." e))
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
                          bgn-idx (+ (str/index-of func-str "$") 1)
                          end-idx (str/index-of func-str "@" bgn-idx)
                          col-func-str (subs func-str bgn-idx end-idx)]
                          (mapv (fn [_] (str col-func-str "(" _ ")")) old-key))
                    ))]
    (cond (not= (count old-key) (count new-key))
          (throw (TypeException. "The number of new keys should equal the number of columns to aggregate.")))
    (let [result (.aggregate this func old-key new-key)]
      (.errorPredetect this "invalid arguments passed to aggregate function")
      result)))

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

(defn rename-col
  [this old-col new-col]
  (let [result (.renameCol this old-col new-col)]
    (.errorPredetect this "invalid arguments passed to rename-col function")
  result))

(defn inner-join
  [a b a-keys b-keys & {:keys [col-prefix] :or {col-prefix ["1" "2"]}}]
  (let [a-keys (u/proc-groupby-key a-keys)
        b-keys (u/proc-groupby-key b-keys)
        a-keys (mapv (fn [_] [(nth _ 0) (get (.getKeyIndex (.col-info a)) (nth _ 1))]) a-keys)
        b-keys (mapv (fn [_] [(nth _ 0) (get (.getKeyIndex (.col-info b)) (nth _ 1))]) b-keys)]
    (cond (not (and (= (type a) clojask.classes.DataFrame.DataFrame) (= (type b) clojask.classes.DataFrame.DataFrame))) 
      (throw (TypeException. "First two arguments should be Clojask dataframes.")))
    (cond (or (not= (.getAggreFunc (:row-info a)) []) (not= (.getGroupbyKeys (:row-info a)) []) (not= (.getAggreFunc (:row-info b)) []) (not= (.getGroupbyKeys (:row-info b)) []))
          (println "The groupby and aggregation operations of the dataframes will be ignored."))
    (cond (not (= (count a-keys) (count b-keys))) 
      (throw (TypeException. "The length of left keys and right keys should be equal.")))
    (cond (some nil? (map second (concat a-keys b-keys))) 
      (throw (TypeException. "Input includes non-existent column name(s).")))
    (let [size-a (.getSize (:stat a))
          size-b (.getSize (:stat b))]
      (if (>= (compare size-a size-b) 0)
        (JoinedDataFrame. a b a-keys b-keys nil nil 1 nil col-prefix false (atom (.getOutput a)))
        (JoinedDataFrame. b a b-keys a-keys nil nil 1 nil [(nth col-prefix 1) (nth col-prefix 0)] true (atom (.getOutput a)))))))

(defn left-join
  [a b a-keys b-keys & {:keys [col-prefix] :or {col-prefix ["1" "2"]}}]
  (let [a-keys (u/proc-groupby-key a-keys)
        b-keys (u/proc-groupby-key b-keys)
        a-keys (mapv (fn [_] [(nth _ 0) (get (.getKeyIndex (.col-info a)) (nth _ 1))]) a-keys)
        b-keys (mapv (fn [_] [(nth _ 0) (get (.getKeyIndex (.col-info b)) (nth _ 1))]) b-keys)]
    (cond (not (and (= (type a) clojask.classes.DataFrame.DataFrame) (= (type b) clojask.classes.DataFrame.DataFrame))) 
      (throw (TypeException. "First two arguments should be Clojask dataframes.")))
    (cond (or (not= (.getAggreFunc (:row-info a)) []) (not= (.getGroupbyKeys (:row-info a)) []) (not= (.getAggreFunc (:row-info b)) []) (not= (.getGroupbyKeys (:row-info b)) []))
          (println "The groupby and aggregation operations of the dataframes will be ignored."))
    (cond (not (= (count a-keys) (count b-keys))) 
      (throw (TypeException. "The length of left keys and right keys should be equal.")))
    (cond (not (= (count col-prefix) 2)) 
      (throw (TypeException. "The length of col-prefix should be equal to 2.")))
    (cond (some nil? (map second (concat a-keys b-keys))) 
      (throw (TypeException. "Input includes non-existent column name(s).")))
    (JoinedDataFrame. a b a-keys b-keys nil nil 2 nil col-prefix false (atom (.getOutput a)))))

(defn right-join
  [a b a-keys b-keys & {:keys [col-prefix] :or {col-prefix ["1" "2"]}}]
  (let [a-keys (u/proc-groupby-key a-keys)
        b-keys (u/proc-groupby-key b-keys)
        a-keys (mapv (fn [_] [(nth _ 0) (get (.getKeyIndex (.col-info a)) (nth _ 1))]) a-keys)
        b-keys (mapv (fn [_] [(nth _ 0) (get (.getKeyIndex (.col-info b)) (nth _ 1))]) b-keys)]
    (cond (not (and (= (type a) clojask.classes.DataFrame.DataFrame) (= (type b) clojask.classes.DataFrame.DataFrame)))
          (throw (TypeException. "First two arguments should be Clojask dataframes.")))
    (cond (or (not= (.getAggreFunc (:row-info a)) []) (not= (.getGroupbyKeys (:row-info a)) []) (not= (.getAggreFunc (:row-info b)) []) (not= (.getGroupbyKeys (:row-info b)) []))
          (println "The groupby and aggregation operations of the dataframes will be ignored."))
    (cond (not (= (count a-keys) (count b-keys)))
          (throw (TypeException. "The length of left keys and right keys should be equal.")))
    (cond (some nil? (map second (concat a-keys b-keys)))
          (throw (TypeException. "Input includes non-existent column name(s).")))
    (JoinedDataFrame. b a b-keys a-keys nil nil 2 nil [(nth col-prefix 1) (nth col-prefix 0)] false (atom (.getOutput a)))))

(defn outer-join
  [a b a-keys b-keys & {:keys [col-prefix] :or {col-prefix ["1" "2"]}}]
  (let [a-keys (u/proc-groupby-key a-keys)
        b-keys (u/proc-groupby-key b-keys)
        a-keys (mapv (fn [_] [(nth _ 0) (get (.getKeyIndex (.col-info a)) (nth _ 1))]) a-keys)
        b-keys (mapv (fn [_] [(nth _ 0) (get (.getKeyIndex (.col-info b)) (nth _ 1))]) b-keys)]
    (cond (not (and (= (type a) clojask.classes.DataFrame.DataFrame) (= (type b) clojask.classes.DataFrame.DataFrame)))
          (throw (TypeException. "First two arguments should be Clojask dataframes.")))
    (cond (or (not= (.getAggreFunc (:row-info a)) []) (not= (.getGroupbyKeys (:row-info a)) []) (not= (.getAggreFunc (:row-info b)) []) (not= (.getGroupbyKeys (:row-info b)) []))
          (println "The groupby and aggregation operations of the dataframes will be ignored."))
    (cond (not (= (count a-keys) (count b-keys)))
          (throw (TypeException. "The length of left keys and right keys should be equal.")))
    (cond (some nil? (map second (concat a-keys b-keys)))
          (throw (TypeException. "Input includes non-existent column name(s).")))
    (let [size-a (.getSize (:stat a))
          size-b (.getSize (:stat b))]
      (if (>= (compare size-a size-b) 0)
        (JoinedDataFrame. a b a-keys b-keys nil nil 3 nil col-prefix false (atom (.getOutput a)))
        (JoinedDataFrame. b a b-keys a-keys nil nil 3 nil [(nth col-prefix 1) (nth col-prefix 0)] true (atom (.getOutput a)))))))

(defn rolling-join-forward
  [a b a-keys b-keys a-roll b-roll & {:keys [col-prefix limit] :or {col-prefix ["1" "2"] limit nil}}]
  (let [a-keys (u/proc-groupby-key a-keys)
        b-keys (u/proc-groupby-key b-keys)
        a-keys (mapv (fn [_] [(nth _ 0) (get (.getKeyIndex (.col-info a)) (nth _ 1))]) a-keys)
        b-keys (mapv (fn [_] [(nth _ 0) (get (.getKeyIndex (.col-info b)) (nth _ 1))]) b-keys)]
    (cond (not (and (= (type a-roll) java.lang.String) (= (type b-roll) java.lang.String)))
          (throw (TypeException. "Rolling keys should be strings")))
    (cond (not (and (= (type a) clojask.classes.DataFrame.DataFrame) (= (type b) clojask.classes.DataFrame.DataFrame)))
          (throw (TypeException. "First two arguments should be Clojask dataframes.")))
    (cond (not (= (count a-keys) (count b-keys)))
          (throw (TypeException. "The length of left keys and right keys should be equal.")))
    (cond (some nil? (map second (concat a-keys b-keys)))
          (throw (TypeException. "Input includes non-existent column name(s).")))
    (cond (or (not= (.getAggreFunc (:row-info a)) []) (not= (.getGroupbyKeys (:row-info a)) []) (not= (.getAggreFunc (:row-info b)) []) (not= (.getGroupbyKeys (:row-info b)) []))
          (println "The groupby and aggregation operations of the dataframes will be ignored."))
    (let [[a-roll b-roll] [(get (.getKeyIndex (:col-info a)) a-roll) (get (.getKeyIndex (:col-info b)) b-roll)]]
      (do
        (cond (not (and (not= a-roll nil) (not= b-roll nil)))
              (throw (TypeException. "Rolling keys include non-existent column name(s).")))
        (JoinedDataFrame. a b a-keys b-keys a-roll b-roll 4 limit col-prefix false (atom (.getOutput a)))))))

;; all of the code is the same as above except for the last line
(defn rolling-join-backward
  [a b a-keys b-keys a-roll b-roll & {:keys [col-prefix limit] :or {col-prefix ["1" "2"] limit nil}}]
  (let [a-keys (u/proc-groupby-key a-keys)
        b-keys (u/proc-groupby-key b-keys)
        a-keys (mapv (fn [_] [(nth _ 0) (get (.getKeyIndex (.col-info a)) (nth _ 1))]) a-keys)
        b-keys (mapv (fn [_] [(nth _ 0) (get (.getKeyIndex (.col-info b)) (nth _ 1))]) b-keys)]
    (cond (not (and (= (type a-roll) java.lang.String) (= (type b-roll) java.lang.String)))
          (throw (TypeException. "Rolling keys should be strings")))
    (cond (not (and (= (type a) clojask.classes.DataFrame.DataFrame) (= (type b) clojask.classes.DataFrame.DataFrame)))
          (throw (TypeException. "First two arguments should be Clojask dataframes.")))
    (cond (or (not= (.getAggreFunc (:row-info a)) []) (not= (.getGroupbyKeys (:row-info a)) []) (not= (.getAggreFunc (:row-info b)) []) (not= (.getGroupbyKeys (:row-info b)) []))
          (println "The groupby and aggregation operations of the dataframes will be ignored."))
    (cond (not (= (count a-keys) (count b-keys)))
          (throw (TypeException. "The length of left keys and right keys should be equal.")))
    (cond (some nil? (map second (concat a-keys b-keys)))
          (throw (TypeException. "Input includes non-existent column name(s).")))
    (let [[a-roll b-roll] [(get (.getKeyIndex (:col-info a)) a-roll) (get (.getKeyIndex (:col-info b)) b-roll)]]
      (do
        (cond (not (and (not= a-roll nil) (not= b-roll nil)))
              (throw (TypeException. "Rolling keys include non-existent column name(s).")))
        (JoinedDataFrame. a b a-keys b-keys a-roll b-roll 5 limit col-prefix false (atom (.getOutput a)))))))

(defn compute
  [this num-worker output-dir & {:keys [exception order output select exclude melt header in-memory] :or {exception false order false output nil select nil exclude nil melt vector header true in-memory false}}]
  (assert (or (nil? select) (nil? exclude)) "Can only specify either of select or exclude")
  (cond (not (integer? num-worker))
        (throw (TypeException. "Number of workers should be an integer.")))
  (cond (< num-worker 1)
        (throw (OperationException. "Number of workers should be at least 1.")))
  (cond (> num-worker 8)
        (throw (OperationException. "Max number of worker nodes is 8.")))
  ;; check if output-dir clashes with input file path
  (.checkInputPathClash this output-dir)
  ;; check which type of dataframe this is
  (let [exclude (if (coll? exclude) exclude [exclude])
        select (if select select (if (not= [nil] exclude) (doall (remove (fn [item] (.contains exclude item)) (.getColNames this))) nil))
        ;; :header is true (the dataframe's own column names), false (no
        ;; header row) or a collection of names that replaces them.
        custom-header (when (coll? header) header)
        header (if custom-header false header)
        ret (atom (transient []))
        output-format (when output-dir (clojask-io.core/infer-format output-dir))
        output-func (if output-dir 
                      ;; every dataframe has an initial output function
                      (if (and (= (type this) clojask.classes.DataFrame.DataFrame) (.getPath this) (= output-format (clojask-io.core/infer-format (.getPath this))))
                        (or output (.getOutput this))
                        (or output (output/get-output-func output-format)))
                      (fn [wtr seq] (doseq [row seq] (reset! ret (conj! (deref ret) row)))))
        ;; all validation must pass before init-file deletes the output file
        _ (assert (not= select []) "Must select at least 1 column")
        _ (assert (or (= melt vector) (and (= (type this) clojask.classes.DataFrame.DataFrame) (= (.getGroupbyKeys (:row-info this)) []) (= (.getAggreFunc (:row-info this)) []))) "melt is not applicable to this dataframe")
        _ (when select
            (let [known (set (.getColNames this))
                  missing (vec (remove known (if (coll? select) select [select])))]
              (when (not= missing [])
                (throw (TypeException. (str "Selected columns do not exist: " (str/join ", " missing) ". get-col-names lists the valid names."))))))
        _ (u/init-file output-dir nil)
        output-dir (or output-dir ".clojask/tmp.csv")] ;; fake one 
    (when custom-header
      (with-open [wrtr (io/writer output-dir)]
        (output-func wrtr [custom-header])))
    (if (= (type this) clojask.classes.DataFrame.DataFrame)
      (if (and (= (.getGroupbyKeys (:row-info this)) []) (= (.getAggreFunc (:row-info this)) []))
        (do ;; simple compute
          (.compute this num-worker output-dir exception order select melt header output-func)
          ) ;; return output dataframe
        (if (not= (.getGroupbyKeys (:row-info this)) [])
          (do ;; groupby-aggre
            (.computeGroupAggre this num-worker output-dir exception select header output-func in-memory))
          (do ;; aggre
            (.computeAggre this num-worker output-dir exception select header output-func))))
      (if (= (type this) clojask.classes.JoinedDataFrame.JoinedDataFrame)
        (do ;; join
          (.compute this num-worker output-dir exception order select header output-func in-memory)
          )
        (throw (TypeException. "Must compute on a clojask dataframe or joined dataframe"))))
    (if (not= output-dir ".clojask/tmp.csv")
      (dataframe output-dir :if-header (boolean (or custom-header header)))
      (persistent! (deref ret)))))

;; ============== Below functions are deprecated ==============

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

(defn sort
  [this list output-dir]
  (u/init-file output-dir nil)
  (.sort this list output-dir))
