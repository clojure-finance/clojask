(ns clojask.classes.JoinedDataFrame
  (:require [clojure.set :as set]
            [clojask.classes.ColInfo]
            [clojask.classes.RowInfo]
            [clojask.classes.DataStat]
            [clojask.classes.MGroup]
            [clojask.classes.DataFrame]
            [clojask.onyx-comps :refer [start-onyx-groupby start-onyx-join]]
            [clojask.join.outer-onyx-comps :refer [start-onyx-outer]]
            [clojure.java.io :as io]
            [clojask.utils])
  (:import [clojask.classes.MGroup MGroupJoin MGroupJoinOuter]
           [clojask.classes.DataFrame GenDFIntf]))

;; ============= Below is the definition for the joineddataframe ================
(definterface JDFIntf
  (compute [^int num-worker ^String output-dir ^boolean exception ^boolean order select ifheader out inmemory]))

(defrecord JoinedDataFrame
           [^clojask.classes.DataFrame.DataFrame a
            ^clojask.classes.DataFrame.DataFrame b
            a-keys
            b-keys
            a-roll
            b-roll
            type
            limit
            prefix
            swapped
            output-func]

  GenDFIntf

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
      ;; a swapped join presents the original argument order: record b is
      ;; the caller's first dataframe
      (if swapped
        (concat b-col-header a-col-header)
        (concat a-col-header b-col-header))))

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
    [this sample-size output-size format]
    (let [data-a (.preview a sample-size output-size format)
          data-b (.preview b sample-size output-size format)
          old-a (.getColNames a)
          old-b (.getColNames b)
          rep-key-a (zipmap old-a (if swapped (take-last (count old-a) (.getColNames this)) (take (count old-a) (.getColNames this))))
          rep-key-b (zipmap old-b (if swapped (take (count old-b) (.getColNames this)) (take-last (count old-b) (.getColNames this))))
          data-a (map #(set/rename-keys % rep-key-a) data-a)
          data-b (map #(set/rename-keys % rep-key-b) data-b)
          data (map (fn [row-a row-b] (merge row-a row-b)) data-a data-b)]
      data))

  JDFIntf

  (compute
    [this ^int num-worker ^String output-dir ^boolean exception ^boolean order select ifheader out inmemory]
    (let [select (if (coll? select) select [select])
          na (count (.getKeyIndex (.col-info a)))
          nb (count (.getKeyIndex (.col-info b)))
          select (if (= select [nil])
                   (vec (take (+ na nb) (iterate inc 0)))
                   (mapv (fn [key] (.indexOf (.getColNames this) key)) select))
          ;; select follows the user-facing getColNames order; internal rows
          ;; are always record-a columns then record-b columns
          internal (if swapped
                     (mapv (fn [i] (if (< i nb) (+ i na) (- i nb))) select)
                     select)
          a-index (vec (apply sorted-set (remove (fn [num] (>= num na)) internal)))
          ;; a-write 
          b-index (mapv #(- % na) (apply sorted-set (remove (fn [num] (< num na)) internal)))
          b-index (if b-roll (vec (apply sorted-set (conj b-index b-roll))) b-index)
          b-roll (if b-roll (count (remove #(>= % b-roll) b-index)) nil)
          ;; b-write
          a-format (clojask.utils/formatters-by-position (.getFormatter (.col-info a)) a-index)
          b-format (clojask.utils/formatters-by-position (.getFormatter (.col-info b)) b-index)
          write-index (mapv (fn [num] (count (remove #(>= % num) (concat a-index (mapv #(+ % na) b-index))))) internal)
          mgroup-a (MGroupJoinOuter. (transient {}) (transient {}) false)
          mgroup-b (if (not= type 3) (MGroupJoin. (transient {}) (transient {}) (or (= 4 type) (= 5 type))) (MGroupJoinOuter. (transient {}) (transient {}) (or (= 4 type) (= 5 type))))
          ]
      (if (= ifheader true) (.printCol this output-dir select out))
      (try
       (cond
         (or (= type 0) (= type 1) (= type 2)) ;; inner left right join
         (do
           (if inmemory
             (start-onyx-groupby num-worker 10 b mgroup-b b-keys b-index exception)
             (start-onyx-groupby num-worker 10 b "./.clojask/join/b/" b-keys b-index exception :format true))
           (.final mgroup-b)
           (start-onyx-join num-worker 10 a b (if inmemory mgroup-b nil) output-dir exception a-keys b-keys a-roll b-roll type limit a-index (vec (take (count b-index) (iterate inc 0))) b-format write-index out))
         (= type 3) ;; outer join
         (do
           (if inmemory
             (do
               (start-onyx-groupby num-worker 10 a mgroup-a a-keys a-index exception)
               (start-onyx-groupby num-worker 10 b mgroup-b b-keys b-index exception)
               (.final mgroup-a)
               )
             (do
               (start-onyx-groupby num-worker 10 a "./.clojask/join/a/" a-keys a-index exception :format true)
               (start-onyx-groupby num-worker 10 b "./.clojask/join/b/" b-keys b-index exception :format true)))
           (start-onyx-outer num-worker 10 a b (if inmemory mgroup-a nil) (if inmemory mgroup-b nil) output-dir exception a-index b-index a-format b-format write-index out))
         (or (= type 4) (= type 5))  ;; rolling join
         (do
           (if inmemory
             (start-onyx-groupby num-worker 10 b mgroup-b b-keys b-index exception)
             (start-onyx-groupby num-worker 10 b "./.clojask/join/b/" b-keys b-index exception))
           (.final mgroup-b)
           (start-onyx-join num-worker 10 a b (if inmemory mgroup-b nil) output-dir exception a-keys b-keys a-roll b-roll type limit a-index (vec (take (count b-index) (iterate inc 0))) b-format write-index out)))
       (finally
         ;; the group files only serve this compute
         (if (not inmemory) (clojask.utils/clean-group-files)))))))

