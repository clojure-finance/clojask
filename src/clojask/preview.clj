(ns clojask.preview
  (:require [clojask.ColInfo :refer [->ColInfo]]
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
            [clojask.preview :as preview]))

(defn preview
  [dataframe sample-size return-size formatting]
  ;; outer loop is the input node
  (let [index-key (.getIndexKey (:col-info dataframe))
        formatters (.getFormatter (:col-info dataframe))
        index (take (count index-key) (iterate inc 0))
        header (mapv index-key index)    ;; the header of the result in sequence vector
        reader (io/reader (:path dataframe))
        csv-data (if (:have-col dataframe)
                   (rest (line-seq reader))
                   (line-seq  reader))
        data (map zipmap (repeat [:clojask-id :data]) (map vector (iterate inc 0) csv-data))
        sample (take sample-size data)    ;; lazy source data (take sample size)
        ;;define the variables needed in the following functions
        operations (.getDesc (:col-info  dataframe))
        types (.getType (:col-info  dataframe))
        filters (.getFilters (:row-info dataframe))
        indices (vec (take (count operations) (iterate inc 0)))
        no-aggre (= (.getAggreFunc (:row-info dataframe)) []) ;; if need to groupby & aggregate
        ;;
        preview-work-func (fn [seg]
                            (let [data (string/split (:data seg) #"," -1)]
                              (if (filter-check filters types data)
                                {:data (mapv (fn [_] (eval-res data types operations _)) indices)}
                                {}))) ;; the function body of operation (take over the work in worker nodes)
        preview-output-func (if (and formatting no-aggre)
                              (fn [row]
                                (mapv (fn [_] (if-let [formatter (get formatters _)]
                                                (formatter (nth (:data row) _))
                                                (nth (:data row) _))) index))
                              (fn [row]
                                (:data row))) ;; the function body of output operation (take over the work in output node) without formatting

        ;; ========== no need to change ===========
        compute-res (loop [rows sample res (transient [])]     ;; the result of normal compute
                      (if (= rows []) ;; exceed sample size
                        (persistent! res)
                        (let [row (first rows)
                              rest (rest rows)
                              row (preview-work-func row)
                              row-res (preview-output-func row)
                              res (conj! res row-res)]
                          (if (>= (count res) return-size)
                            (persistent! res)
                            (recur rest res)))))]
    (if no-aggre
      (mapv (fn [row-v] (zipmap header row-v)) compute-res)
      ;; need to do groupby and aggregate
       (let [groupby-keys (.getGroupbyKeys (:row-info dataframe))
             key-index (.getKeyIndex (:col-info dataframe))
             groupby-res (loop [sample compute-res groupby {}]
                           (if-let [row (first sample)]
                             (let [res (rest sample)
                                   key (gen-groupby-filenames "" row groupby-keys key-index formatters)]
                               (recur res (assoc groupby key (conj (or (get groupby key) []) row))))
                             groupby))
             aggre-funcs (.getAggreFunc (.row-info dataframe))
             groupby-key-index (.getGroupbyKeys (:row-info dataframe))
             groupby-keys (vec (map (.getIndexKey (.col-info dataframe)) groupby-key-index))
             aggre-new-keys (.getAggreNewKeys (:row-info dataframe))
             keys (concat groupby-keys aggre-new-keys)
             preview-aggre-func (fn [key v-of-v]
                                  (let [data v-of-v
                                        ;; pre 
                                        pre (mapv #(nth (first v-of-v) %) groupby-key-index)
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
             (mapv (fn [row-v] (zipmap keys row-v)) aggre-res)))
         )
      )))