(ns aggregate.aggre-onyx-comps
  (:require [aggregate.aggre-input :as input]
            [aggregate.aggre-output :as output]
            ;; [clojask.clojask-groupby :as groupby]
            ;; [clojask.clojask-join :as join]
            [onyx.api :refer :all]
            [clojure.string :as string]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            ;; [tech.v3.dataset :as ds]
            [clojure.data.csv :as csv]
            [clojask.utils :as u]
            [clojure.set :as set]
            [clojask.groupby :refer [read-csv-seq]])
  (:import (java.io BufferedReader FileReader BufferedWriter FileWriter)))


(def id (java.util.UUID/randomUUID))

(defn workflow-gen
  "Generate workflow for running Onyx"
  [num-work]
  (def workflow []) ;; initialisation

  ;; for loop for input edges
  (doseq [x (range 1 (+ num-work 1))]
    (let [worker-name (keyword (str "sample-worker" x))]
          (def workflow (conj workflow [:in worker-name]
              ))))

  ;; for loop for output edges
  (doseq [x (range 1 (+ num-work 1))]
    (let [worker-name (keyword (str "sample-worker" x))]
          (def workflow (conj workflow [worker-name :output]
              ))))
)

(def dataframe (atom nil))


(defn worker-func-gen
  [df exception aggre-funcs index formatter]
  (reset! dataframe df)
  (let [
        ;; aggre-funcs (.getAggreFunc (.row-info (deref dataframe)))
        formatters formatter
        ;; key-index (.getKeyIndex (.col-info (deref dataframe)))
        ;; formatters (set/rename-keys formatters key-index)
        ]
    (defn worker-func
      "refered in preview"
      [seq]
      ;; (println formatters)
      (let [data (read-csv-seq (:file seq))
            pre (:d seq)
            data-map (-> (iterate inc 0)
                         (zipmap (apply map vector data)))
            reorder (fn [a b]
                      ;; (println [a b])
                      (u/gets (concat a b) index))]
        ;; (mapv (fn [_]
        ;;        (let [func (first _)
        ;;              index (nth _ 1)]
        ;;          (func (get data-map index))))
        ;;      aggre-funcs)
        (loop [aggre-funcs aggre-funcs
               res []]
          (if (= aggre-funcs [])
            ;; {:d (vec (concat pre res))}
            (if (= res [])
              {:d [(u/gets pre index)]}
              {:d (mapv reorder (repeat pre) (apply map vector res))})
            (let [func (first (first aggre-funcs))
                  index (nth (first aggre-funcs) 1)
                  res-funcs (rest aggre-funcs)
                  new (func (get data-map index))
                  new (if (coll? new)
                        new
                        (vector new))
                  new (mapv (fn [_] (if-let [formatter (get formatters index)]
                                     (formatter _)
                                     _)) new)]
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
      :onyx/plugin :aggregate.aggre-input/input
      :onyx/type :input
      :onyx/medium :seq
      :seq/checkpoint? true
      :onyx/batch-size batch-size
      :onyx/max-peers 1
      :input/doc "Reads segments from a core.async channel"}))

    ;; for loop for sample workers
    (doseq [x (range 1 (+ num-work 1))]
      (let [worker-name (keyword (str "sample-worker" x))
            worker-function (keyword "aggregate.aggre-onyx-comps" "worker-func")]
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
        :onyx/plugin :aggregate.aggre-output/output
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
      :lifecycle/calls :aggregate.aggre-input/reader-calls}
     {:lifecycle/task :output
      :buffered-wtr/filename dist
      :lifecycle/calls :aggregate.aggre-output/writer-calls}]))

(def num-workers (atom 1))

(defn rem0?
  [event old-segment new-segment all-new-segment]
  ;; (spit "resources/debug.txt" (str new-segment "\n") :append true)
  (= (mod (:id new-segment) (deref num-workers)) 0))

(defn rem1?
  [event old-segment new-segment all-new-segment]
  (= (mod (:id new-segment) (deref num-workers)) 1))

(defn rem2?
  [event old-segment new-segment all-new-segment]
  (= (mod (:id new-segment) (deref num-workers)) 2))

(defn rem3?
  [event old-segment new-segment all-new-segment]
  (= (mod (:id new-segment) (deref num-workers)) 3))

(defn rem4?
  [event old-segment new-segment all-new-segment]
  (= (mod (:id new-segment) (deref num-workers)) 4))

(defn rem5?
  [event old-segment new-segment all-new-segment]
  (= (mod (:id new-segment) (deref num-workers)) 5))

(defn rem6?
  [event old-segment new-segment all-new-segment]
  (= (mod (:id new-segment) (deref num-workers)) 6))

(defn rem7?
  [event old-segment new-segment all-new-segment]
  (= (mod (:id new-segment) (deref num-workers)) 7))

(defn rem8?
  [event old-segment new-segment all-new-segment]
  (= (mod (:id new-segment) (deref num-workers)) 8))


;; [{:flow/from :in
;;   :flow/to [:sample-worker1]
;;   :flow/predicate :clojask.onyx-comps/rem0?
;;   :flow/doc ""}
;;  {:flow/from :in
;;   :flow/to [:sample-worker2]
;;   :flow/predicate :clojask.onyx-comps/rem1?
;;   :flow/doc ""}]

(defn flow-cond-gen
  "Generate the flow conditions for running Onyx"
  [num-work]
  (reset! num-workers num-work)
  (def flow-conditions []) ;; initialisation

  ;; for loop for sample workers
  (doseq [x (range 1 (+ num-work 1))]
    (let [worker-name (keyword (str "sample-worker" x))
          predicate-function (keyword "aggregate.aggre-onyx-comps" (str "rem" (- x 1) "?"))]
          (def flow-conditions
            (conj flow-conditions
             {:flow/from :in
              :flow/to [worker-name]
              :flow/predicate predicate-function
              :worker/doc "This is a flow condition"}
              ))))
    
  ;; (println flow-conditions) ;; !! debugging
  )

(defn config-env
  []
  (def env-config
    {:zookeeper/address "127.0.0.1:2188"
     :zookeeper/server? true
     :zookeeper.server/port 2188
     :onyx/tenancy-id id
     :onyx.log/file "_clojask/clojask.log"})

  (def peer-config
    {:zookeeper/address "127.0.0.1:2188"
     :onyx/tenancy-id id
     :onyx.peer/job-scheduler :onyx.job-scheduler/balanced
     :onyx.messaging/impl :aeron
     :onyx.messaging/peer-port 40200
     :onyx.messaging/bind-addr "localhost"
     :onyx.log/file "_clojask/clojask.log"})

  (def env (onyx.api/start-env env-config))

  (def peer-group (onyx.api/start-peer-group peer-config))

  (def n-peers (count (set (mapcat identity workflow))))

  (def v-peers (onyx.api/start-peers n-peers peer-group)))

(defn shutdown
  []
  (doseq [v-peer v-peers]
    (onyx.api/shutdown-peer v-peer))
  (onyx.api/shutdown-peer-group peer-group)
  (onyx.api/shutdown-env env))

(defn start-onyx-aggre
  "start the onyx cluster with the specification inside dataframe"
  [num-work batch-size dataframe dist exception aggre-func index formatter]
  (try
    (workflow-gen num-work)
    (config-env)
    (worker-func-gen dataframe exception aggre-func index formatter) ;;need some work
    (catalog-gen num-work batch-size)
    (lifecycle-gen "./_clojask/grouped" dist)
    (flow-cond-gen num-work)
    (input/inject-dataframe dataframe)

    (catch Exception e (throw (Exception. (str "[preparing stage] " (.getMessage e))))))
  (try
    (let [submission (onyx.api/submit-job peer-config
                                          {:workflow workflow
                                           :catalog catalog
                                           :lifecycles lifecycles
                                           :flow-conditions flow-conditions
                                           :task-scheduler :onyx.task-scheduler/balanced})
          job-id (:job-id submission)]
      ;; (println submission)
      (assert job-id "Job was not successfully submitted")
      (feedback-exception! peer-config job-id))
    (catch Exception e (do
                         (shutdown)
                         (throw (Exception. (str "[submit-to-onyx stage] " (.getMessage e)))))))
  (try
    (shutdown)
    (catch Exception e (throw (Exception. (str "[terminate-node stage] " (.getMessage e))))))
  "success")
