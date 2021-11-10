(ns clojask.preview
  (:require [clojure.set :as set]
            [clojask.ColInfo :refer [->ColInfo]]
            [clojask.RowInfo :refer [->RowInfo]]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojask.utils :refer [eval-res eval-res-ne filter-check]]
            [clojask.groupby :refer [internal-aggregate aggre-min gen-groupby-filenames]]
            [clojask.onyx-comps :refer [start-onyx start-onyx-groupby start-onyx-join]]
            [clojask.sort :as sort]
            [clojask.join :as join]
            [clojure.string :as string]
            [aggregate.aggre-onyx-comps :refer [start-onyx-aggre]]
            [clojure.string :as str]
            [clojask.preview :as preview]
            [clojask.api.aggregate :as aggre]))

(defn preview
  [dataframe sample-size return-size formatting]
  ;; outer loop is the input node
  (let [index-key (.getIndexKey (:col-info dataframe))
        formatters (.getFormatter (:col-info dataframe))
        ;index (take (count index-key) (iterate inc 0))
        indices-deleted (.getDeletedCol (:col-info dataframe))
        indices-wo-del (vec (take (count index-key) (iterate inc 0)))
        indices-not-deleted (set/difference (set indices-wo-del) (set indices-deleted))
        index (if (empty? indices-deleted) 
                  indices-wo-del ;; no columns deleted
                  ;(vec (set/difference (set indices-wo-del) (set indices-deleted))) ; minus column indices deleted
                  (filterv (fn [i] (contains? indices-not-deleted i)) indices-wo-del)
                  )
        header (mapv index-key index)    ;; the header of the result in sequence vector
        reader (io/reader (:path dataframe))
        csv-data (if (:have-col dataframe)
                   (rest (line-seq reader))
                   (line-seq  reader))
        data (map zipmap (repeat [:id :d]) (map vector (iterate inc 0) csv-data))
        sample (take sample-size data)    ;; lazy source data (take sample size)
        ;;define the variables needed in the following functions
        operations (.getDesc (:col-info  dataframe))
        types (.getType (:col-info  dataframe))
        filters (.getFilters (:row-info dataframe))
        indices index
        no-aggre (= (.getAggreFunc (:row-info dataframe)) []) ;; if need to groupby & aggregate
        no-groupby (= (.getGroupbyKeys (:row-info dataframe)) [])
        ;;
        preview-work-func (fn [seg]
                            (let [data (string/split (:d seg) #"," -1)]
                              (if (filter-check filters types data)
                                {:d (mapv (fn [_] (eval-res data types operations _)) indices)}
                                {}))) ;; the function body of operation (take over the work in worker nodes)
        preview-output-func (if (and formatting no-aggre)
                              (fn [row]
                                (mapv (fn [_] (if-let [formatter (get formatters _)]
                                                (formatter (nth (:d row) _))
                                                (nth (:d row) _))) index))
                              (fn [row]
                                (:d row))) ;; the function body of output operation (take over the work in output node) without formatting

        ;; ========== no need to change ===========
        compute-res (loop [rows sample res (transient [])]     ;; the result of normal compute
                      (if (= rows []) ;; exceed sample size
                        (persistent! res)
                        (let [row (first rows)
                              rest (rest rows)
                              row (preview-work-func row)
                              row-res (preview-output-func row)
                              res (if row-res (conj! res row-res) res)]
                          (if (>= (count res) return-size)
                            (persistent! res)
                            (recur rest res)))))]
    ;; debug
    (println header)
    (println index)
    (if no-aggre
      (mapv (fn [row-v] (zipmap header row-v)) compute-res)
      ;; need to do aggregate
      (if no-groupby
        ;; need to do simple aggregate
        (let [aggre-funcs (.getAggreFunc (.row-info dataframe))
              keys (.getAggreNewKeys (:row-info dataframe))
              aggre-res (for [[func index] aggre-funcs]
                          (let [res
                                (reduce func aggre/start (map (fn [row] (nth row index)) compute-res))]
                            (if (coll? res)
                              res
                              [res])))]
          (if (apply = (map count aggre-res))
            (mapv (fn [row-v] (zipmap keys row-v)) (apply map vector aggre-res))
            (throw (Exception. "aggregation result is not of the same length"))))
        ;; need to do groupby aggregate
        (let [groupby-keys (.getGroupbyKeys (:row-info dataframe))
              key-index (.getKeyIndex (:col-info dataframe))
              index-key (.getIndexKey (.col-info dataframe))
              groupby-res (loop [sample compute-res groupby {}]
                            (if-let [row (first sample)]
                              (let [res (rest sample)
                                    key (gen-groupby-filenames "" row groupby-keys key-index formatters)]
                                (recur res (assoc groupby key (conj (or (get groupby key) []) row))))
                              groupby))
              aggre-funcs (.getAggreFunc (.row-info dataframe))
            ;;  groupby-key-index (mapv #(nth % 1) (.getGroupbyKeys (:row-info dataframe)))
              groupby-keys-value (vec (map #(if (nth % 0)
                                              (str (nth % 0) "(" (index-key (nth % 1)) ")")
                                              (index-key (nth % 1))) groupby-keys))
              aggre-new-keys (.getAggreNewKeys (:row-info dataframe))
              keys (concat groupby-keys-value aggre-new-keys)
              preview-aggre-func (fn [key v-of-v]
                                   (let [data v-of-v
                                        ;; pre 
                                         pre (mapv #(let [func (first %)
                                                          index (nth % 1)]
                                                      (if func
                                                        (func (nth (first v-of-v) index))
                                                        (if formatting
                                                          ((or (get formatters index) identity) (nth (first v-of-v) index))
                                                          (nth (first v-of-v) index))))
                                                   groupby-keys)
                                         data-map (-> (iterate inc 0)
                                                      (zipmap (apply map vector data)))]
                                     (loop [aggre-funcs aggre-funcs
                                            res []]
                                       (if (= aggre-funcs [])
                                         (mapv concat (repeat pre) (apply map vector res))
                                         (let [func (first (first aggre-funcs))
                                               index (nth (first aggre-funcs) 1)
                                               res-funcs (rest aggre-funcs)
                                               new (func (get data-map index))
                                               new (if (coll? new)
                                                     new
                                                     (vector new))
                                               new (if formatting
                                                     (mapv (fn [_] (if-let [formatter (get formatters index)]
                                                                     (formatter _)
                                                                     _)) new)
                                                     new)]
                                           (if (or (= res []) (= (count new) (count (last res))))
                                             (recur res-funcs (conj res new))
                                             (throw (Exception. "aggregation result is not of the same length"))))))))]
          (loop [groupby-res groupby-res aggre-res []]
            (if-let [key-vv (first groupby-res)]
              (let [res (rest groupby-res)
                    key (nth key-vv 0)
                    vv (nth key-vv 1)]
                (recur res (concat aggre-res (preview-aggre-func key vv))))
              (mapv (fn [row-v] (zipmap keys row-v)) aggre-res))))))))