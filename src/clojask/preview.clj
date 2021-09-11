(ns clojask.preview
  (:require [clojask.ColInfo :refer [->ColInfo]]
            [clojask.RowInfo :refer [->RowInfo]]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojask.utils :refer [eval-res eval-res-ne filter-check]]
            [clojask.groupby :refer [internal-aggregate aggre-min]]
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
        header (mapv index-key (take (count index-key) (iterate inc 0)))    ;; the header of the result in sequence vector
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
        ;;
        preview-work-func (fn [seg]
                            (let [data (string/split (:data seg) #"," -1)]
                              (if (filter-check filters types data)
                                {:data (mapv (fn [_] (eval-res data types operations _)) indices)}
                                {}))) ;; the function body of operation (take over the work in worker nodes)
        preview-output-func (if formatting
                              (fn [row]
                                nil)
                              (fn [row]
                                (:data row))) ;; the function body of output operation (take over the work in output node) without formatting

        ;; ========== no need to change ===========
        no-aggre (= (.getAggreFunc (:row-info dataframe)) []) ;; if need to groupby & aggregate
        compute-res (loop [rows sample res (transient [])]     ;; the result of normal compute
                      (if (= rows []) ;; exceed sample size
                        (persistent! res)
                        (let [row (first rows)
                              rest (rest rows)
                              row (preview-work-func row)
                              row-res (zipmap header (preview-output-func row))
                              res (conj! res row-res)]
                          (if (>= (count res) return-size)
                            (persistent! res)
                            (recur rest res)))))]
    (if no-aggre
      compute-res
      ;; need to do groupby and aggregate
      (
       
      ))))