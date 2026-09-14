(ns clojask.join.outer-onyx-comps
  (:require [clojask.join.outer-input :as input]
            [clojask.join.outer-output :as output]
            [clojask.onyx-comps :as oc]
            [clojure.string :as string]
            [clojask.utils :as u]
            [clojure.java.io :as io]
            [clojask.groupby :refer [read-csv-seq]]))

(def dataframe (atom nil))

(defn worker-func-gen
  [a b mgroup-a mgroup-b exception a-index b-index a-format b-format write-index]
  (let [a-count (count a-index)
        b-count (count b-index)
        b-nil (repeat b-count nil)
        add-nil (fn [row] (concat row b-nil))
        a-index-new (take (count a-index) (iterate inc 0))
        b-index-new (take (count b-index) (iterate inc 0))
        ]
    (if (= nil mgroup-a)
      (defn worker-func
        "refered in preview"
        [seq]
        (let [id (:id seq)
              a-filename (:d seq)
              a-data (read-csv-seq a-filename)
              a-data (map #(u/gets % a-index-new) a-data)
              b-filename (string/replace-first a-filename "/a/" "/b/")]
          (if (.exists (io/file b-filename))
            (do
              (let [b-data (mapv #(u/gets % b-index-new) (read-csv-seq b-filename))]
                (io/delete-file b-filename true)
                {:id id :d (mapv #(u/gets % write-index) (for [a-row a-data b-row b-data] (concat a-row b-row)))}) ;; formatter here
              )
            {:id id :d (mapv #(u/gets % write-index) (map add-nil a-data))})))
      (defn worker-func
        "refered in preview"
        [seq]
        (let [id (:id seq)
              a-filename (:d seq)
              a-data (.getKey mgroup-a a-filename)
              a-data (map #(u/gets % a-index-new) a-data)
              b-filename a-filename
              ]
          (if (.exists mgroup-b b-filename)
            (do
              (let [b-data (mapv #(u/gets % b-index-new) (.getKey mgroup-b b-filename))]
                {:id id :d (mapv #(u/gets % write-index) (for [a-row a-data b-row b-data] (concat a-row b-row)))}) ;; formatter here
              )
            {:id id :d (mapv #(u/gets % write-index) (map add-nil a-data))}))))))

(defn worker-func-gen2
  [a b mgroup-a mgroup-b exception a-index b-index a-format b-format write-index]
  (let [a-count (count a-index)
        b-count (count b-index)
        a-nil (repeat a-count nil)
        add-nil (fn [row] (concat a-nil row))
        b-index-new (take (count b-index) (iterate inc 0))]
    (if (= mgroup-a nil)
      (defn worker-func
        "refered in preview"
        [seq]
        (let [id (:id seq)
              b-filename (:d seq)
              b-data (mapv #(u/gets % b-index-new) (read-csv-seq b-filename))]
          {:id id :d (mapv #(u/gets % write-index) (mapv add-nil b-data))}))
      (defn worker-func
        "refered in preview"
        [seq]
        (let [id (:id seq)
              b-filename (:d seq)
              b-data (mapv #(u/gets % b-index-new) (.getKey mgroup-b b-filename))]
          {:id id :d (mapv #(u/gets % write-index) (mapv add-nil b-data))})))))

(defn catalog-gen
  "Generate the catalog for running Onyx"
  [num-work batch-size]
  ;; initialisation
  (def catalog [])

  ;; input
  (def catalog 
    (conj catalog
     {:onyx/name :in
      :onyx/plugin :clojask.join.outer-input/input
      :onyx/type :input
      :onyx/medium :seq
      :seq/checkpoint? true
      :onyx/batch-size batch-size
      :onyx/max-peers 1
      :input/doc "Reads segments from a core.async channel"}))

    ;; for loop for sample workers
    (doseq [x (range 1 (+ num-work 1))]
      (let [worker-name (keyword (str "sample-worker" x))
            worker-function (keyword "clojask.join.outer-onyx-comps" "worker-func")]
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
        :onyx/plugin :clojask.join.outer-output/output
        :onyx/type :output
        :onyx/medium :core.async  ;; this is maked up
        :onyx/max-peers 1
        :onyx/batch-size batch-size
        :output/doc "Writes segments to the file"}))

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
      :lifecycle/calls :clojask.join.outer-input/reader-calls}
     {:lifecycle/task :output
      :buffered-wtr/filename dist
      :lifecycle/calls :clojask.join.outer-output/writer-calls}]))

(defn flow-cond-gen
  "Generate the flow conditions for running Onyx"
  [num-work]
  (def flow-conditions []) ;; initialisation

  ;; for loop for sample workers
  (doseq [x (range 1 (+ num-work 1))]
    (let [worker-name (keyword (str "sample-worker" x))
          predicate-function (keyword "clojask.join.outer-onyx-comps" (str "rem" (- x 1) "?"))]
      (intern 'clojask.join.outer-onyx-comps (symbol (str "rem" (- x 1) "?")) (fn [event old-segment new-segment all-new-segment]
                                                                     (= (mod (:id new-segment) num-work) (- x 1))))    
      (def flow-conditions
            (conj flow-conditions
             {:flow/from :in
              :flow/to [worker-name]
              :flow/predicate predicate-function
              :worker/doc "This is a flow condition"}
              ))))
    
  )

(defn start-onyx-outer
  "start the onyx cluster with the specification inside dataframe"
  [num-work batch-size a b mgroup-a mgroup-b dist exception a-index b-index a-format b-format write-index output]
  (oc/with-onyx-env "outer join" num-work
    (fn []
      ;; step 1: every row of a, with its matches in b
      (oc/run-job "outer join"
                  (fn []
                    (worker-func-gen a b mgroup-a mgroup-b exception a-index b-index a-format b-format write-index)
                    (catalog-gen num-work batch-size)
                    (lifecycle-gen "./.clojask/join/a" dist)
                    (flow-cond-gen num-work)
                    (input/inject-dataframe mgroup-a mgroup-b)
                    (output/inject-write-func output)
                    {:catalog catalog :lifecycles lifecycles :flow-conditions flow-conditions}))
      ;; step 2: the rows of b that had no match, on the same environment
      (oc/run-job "outer join 2"
                  (fn []
                    (if (not= mgroup-b nil) (.final mgroup-b))
                    (worker-func-gen2 a b mgroup-a mgroup-b exception a-index b-index a-format b-format write-index)
                    (lifecycle-gen "./.clojask/join/b" dist)
                    (input/inject-dataframe mgroup-b nil)
                    {:catalog catalog :lifecycles lifecycles :flow-conditions flow-conditions})))))