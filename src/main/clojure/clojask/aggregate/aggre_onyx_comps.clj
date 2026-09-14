(ns clojask.aggregate.aggre-onyx-comps
  (:require [clojask.aggregate.aggre-input :as input]
            [clojask.aggregate.aggre-output :as output]
            ;; [clojask.clojask-groupby :as groupby]
            ;; [clojask.clojask-join :as join]
            [clojask.onyx-comps :as oc]
            [clojure.string :as string]
            ;; [tech.v3.dataset :as ds]
            [clojure.data.csv :as csv]
            [clojask.utils :as u]
            [clojure.set :as set]
            [clojask.groupby :refer [read-csv-seq]])
  (:import (java.io BufferedReader FileReader BufferedWriter FileWriter)
           [com.clojask.exception ExecutionException]))

(def dataframe (atom nil))

(defn worker-func-gen
  [df exception aggre-funcs index formatter source]
  (reset! dataframe df)
  (let [
        ;; aggre-funcs (.getAggreFunc (.row-info (deref dataframe)))
        formatters formatter
        ;; key-index (.getKeyIndex (.col-info (deref dataframe)))
        ;; formatters (set/rename-keys formatters key-index)
        reorder (fn [a b]
                      ;; (println [a b])
                  (u/gets (concat a b) index))
        groupby-keys (.getGroupbyKeys (:row-info df))
        ;; tmp (println groupby-keys)
        groupby-index (mapv #(nth % 1) groupby-keys)
        org-format (set/rename-keys (.getFormatter (:col-info df)) (zipmap groupby-index (iterate inc 0)))
        pre-index (take (count groupby-index) (iterate inc 0))
        ]
    (defn worker-func
      "refered in preview"
      [seq]
      ;; (println formatters)
      (let [data (if (= source nil) (read-csv-seq (:file seq)) (.getKey source (:file seq)))
            pre (:d seq)
            pre (u/gets-format pre pre-index org-format)
            data-map (-> (iterate inc 0)
                         (zipmap (apply map vector data)))]
        ;; (mapv (fn [_]
        (loop [aggre-funcs aggre-funcs
               res []]
          (if (= aggre-funcs [])
            ;; {:d (vec (concat pre res))}
            (if (= res [])
              {:d [pre]}
              {:d (mapv reorder (repeat pre) (apply map vector res))})
            (let [func (first (first aggre-funcs))
                  index (nth (first aggre-funcs) 1)
                  res-funcs (rest aggre-funcs)
                  ;; tmp (println index)
                  ;; tmp (println (str data-map))
                  new (func (get data-map index))
                  new (if (coll? new)
                        new
                        (vector new))
                  new (mapv (fn [_] (if-let [formatter (get formatters index)]
                                     (formatter _)
                                     (str _))) new)]
              (if (or (= res []) (= (count new) (count (last res))))
                (recur res-funcs (conj res new))
                (throw (Exception. "aggregation result is not of the same length")))
              )))
        ))))

(defn catalog-gen
  "Generate the catalog for running Onyx"
  [num-work batch-size]
  ;; initialisation
  (def catalog [])

  ;; input
  (def catalog 
    (conj catalog
     {:onyx/name :in
      :onyx/plugin :clojask.aggregate.aggre-input/input
      :onyx/type :input
      :onyx/medium :seq
      :seq/checkpoint? true
      :onyx/batch-size batch-size
      :onyx/max-peers 1
      :input/doc "Reads segments from a core.async channel"}))

    ;; for loop for sample workers
    (doseq [x (range 1 (+ num-work 1))]
      (let [worker-name (keyword (str "sample-worker" x))
            worker-function (keyword "clojask.aggregate.aggre-onyx-comps" "worker-func")]
            (def catalog 
              (conj catalog
               {:onyx/name worker-name
                :onyx/fn worker-function
                :onyx/type :function
                :onyx/batch-size batch-size
                :worker/doc "This is a worker node"}
                ))))
    
    ;; output
    (def catalog
      (conj catalog
      {:onyx/name :output
        :onyx/plugin :clojask.aggregate.aggre-output/output
        :onyx/type :output
        :onyx/medium :core.async  ;; this is maked up
        :onyx/max-peers 1
        :onyx/batch-size batch-size
        :output/doc "Writes segments to the file"}))

    ;; (println catalog) ;; !! debugging
    )

(defn inject-in-reader [event lifecycle]
  (let []
    {:buffered-reader/path (:buffered-reader/path lifecycle)
     }))

(def in-calls
  {:lifecycle/before-task-start inject-in-reader})

(defn lifecycle-gen
  [source dist]
  (def lifecycles
    [{:lifecycle/task :in
      :buffered-reader/path source
      :lifecycle/calls ::in-calls}
     {:lifecycle/task :in
      :lifecycle/calls :clojask.aggregate.aggre-input/reader-calls}
     {:lifecycle/task :output
      :buffered-wtr/filename dist
      :lifecycle/calls :clojask.aggregate.aggre-output/writer-calls}]))

(defn flow-cond-gen
  "Generate the flow conditions for running Onyx"
  [num-work]
  (def flow-conditions []) ;; initialisation

  ;; for loop for sample workers
  (doseq [x (range 1 (+ num-work 1))]
    (let [worker-name (keyword (str "sample-worker" x))
          predicate-function (keyword "clojask.aggregate.aggre-onyx-comps" (str "rem" (- x 1) "?"))]
      (intern 'clojask.aggregate.aggre-onyx-comps (symbol (str "rem" (- x 1) "?")) (fn [event old-segment new-segment all-new-segment]
                                                                     (= (mod (:id new-segment) num-work) (- x 1))))    
      (def flow-conditions
            (conj flow-conditions
             {:flow/from :in
              :flow/to [worker-name]
              :flow/predicate predicate-function
              :worker/doc "This is a flow condition"}
              ))))
    
  ;; (println flow-conditions) ;; !! debugging
  )

(defn start-onyx-aggre
  "start the onyx cluster with the specification inside dataframe"
  [num-work batch-size dataframe source dist exception aggre-func index formatter out]
  (oc/with-onyx-env "groupby aggregate" num-work
    (fn []
      (oc/run-job "groupby aggregate"
                  (fn []
                    (worker-func-gen dataframe exception aggre-func index formatter source)
                    (catalog-gen num-work batch-size)
                    (lifecycle-gen (if (nil? source) "./.clojask/grouped" nil) dist)
                    (flow-cond-gen num-work)
                    (input/inject-dataframe dataframe source)
                    (output/inject-dataframe dataframe out)
                    {:catalog catalog :lifecycles lifecycles :flow-conditions flow-conditions})))))