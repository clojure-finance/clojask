(ns clojask.classes.JoinedDataFrame
  (:require [clojure.set :as set]
            [clojask.classes.ColInfo :refer [->ColInfo]]
            [clojask.classes.RowInfo :refer [->RowInfo]]
            [clojask.classes.DataStat :refer [->DataStat]]
            [clojask.classes.MGroup :refer [->MGroupJoin]]
            [clojask.classes.DataFrame :refer [->DataFrame]]
            [clojask.onyx-comps :refer [start-onyx start-onyx-aggre-only start-onyx-groupby start-onyx-join]]
            ;; [clojask.aggregate.aggre-onyx-comps :refer [start-onyx-aggre]]
            [clojask.join.outer-onyx-comps :refer [start-onyx-outer]]
            [clojure.java.io :as io]
            [clojask.utils :as u])
  (:import
   [clojask.classes.ColInfo ColInfo]
   [clojask.classes.RowInfo RowInfo]
   [clojask.classes.DataStat DataStat]
   [clojask.classes.MGroup MGroupJoin]
   [clojask.classes.DataFrame GenDFIntf DataFrame]
   [com.clojask.exception TypeException OperationException]))

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
    [this sample-size output-size format]
    (let [data-a (.preview a sample-size output-size format)
          data-b (.preview b sample-size output-size format)
          old-a (.getColNames a)
          old-b (.getColNames b)
          rep-key-a (zipmap old-a (take (count old-a) (.getColNames this)))
          rep-key-b (zipmap old-b (take-last (count old-b) (.getColNames this)))
          data-a (map #(set/rename-keys % rep-key-a) data-a)
          data-b (map #(set/rename-keys % rep-key-b) data-b)
          data (map (fn [row-a row-b] (merge row-a row-b)) data-a data-b)]
      data))

  JDFIntf

  (compute
    [this ^int num-worker ^String output-dir ^boolean exception ^boolean order select ifheader out inmemory]
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
          mgroup-a (MGroupJoin. (transient {}) (transient {}))
          mgroup-b (MGroupJoin. (transient {}) (transient {}))
          ]
      ;; (u/init-file output-dir)
      ;; print column names
      (if (= ifheader true) (.printCol this output-dir select out))
      (if (not= type 3)
        (do
          (if inmemory
            (start-onyx-groupby num-worker 10 b mgroup-b b-keys b-index exception)
            (start-onyx-groupby num-worker 10 b "./.clojask/join/b/" b-keys b-index exception))
          (.final mgroup-b)
          (start-onyx-join num-worker 10 a b (if inmemory mgroup-b nil) output-dir exception a-keys b-keys a-roll b-roll type limit a-index (vec (take (count b-index) (iterate inc 0))) b-format write-index out))
        (do
          (start-onyx-groupby num-worker 10 a "./.clojask/join/a/" a-keys a-index exception)
          (start-onyx-groupby num-worker 10 b "./.clojask/join/b/" b-keys b-index exception)
          (start-onyx-outer num-worker 10 a b output-dir exception a-index b-index a-format b-format write-index out))))))

