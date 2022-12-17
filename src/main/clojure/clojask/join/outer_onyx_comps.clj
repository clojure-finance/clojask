(ns clojask.join.outer-onyx-comps
  (:require [clojask.join.outer-input :as input]
            [clojask.join.outer-output :as output]
            [onyx.api :refer :all]
            [clojure.string :as string]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            [clojure.data.csv :as csv]
            [clojask.utils :as u]
            [clojure.set :as set]
            [clojure.java.io :as io]
            [clojask.groupby :refer [read-csv-seq]])
  (:import (java.io BufferedReader FileReader BufferedWriter FileWriter)
           [com.clojask.exception ExecutionException]))


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
  [a b mgroup-a mgroup-b exception a-index b-index a-format b-format write-index]
  (let [a-count (count a-index)
        b-count (count b-index)
        b-nil (repeat b-count nil)
        add-nil (fn [row] (concat row b-nil))
        a-index-new (take (count a-index) (iterate inc 0))
        b-index-new (take (count b-index) (iterate inc 0))
        ;; a-format (.getFormatter (:col-info a))
        ;; a-format (set/rename-keys a-format (zipmap (deref a-index) (iterate inc 0)))
        ;; a-format (.getFormatter (:col-info a))
        ;; a-format (set/rename-keys a-format (zipmap (deref a-index) (iterate inc 0)))
        ]
    (if (= nil mgroup-a)
      (defn worker-func
        "refered in preview"
        [seq]
      ;; (println seq)
        (let [id (:id seq)
              a-filename (:d seq)
              a-data (read-csv-seq a-filename)
              a-data (map #(u/gets-format % a-index-new a-format) a-data)
              b-filename (string/replace-first a-filename "/a/" "/b/")]
          (if (.exists (io/file b-filename))
            (do
              (let [b-data (mapv #(u/gets-format % b-index-new b-format) (read-csv-seq b-filename))]
                (io/delete-file b-filename true)
                {:id id :d (mapv #(u/gets % write-index) (for [a-row a-data b-row b-data] (concat a-row b-row)))}) ;; formatter here
              )
            {:id id :d (mapv #(u/gets % write-index) (map add-nil a-data))})))
      (defn worker-func
        "refered in preview"
        [seq]
      ;; (println seq)
        (let [id (:id seq)
              a-filename (:d seq)
              a-data (.getKey mgroup-a a-filename)
              a-data (map #(u/gets % a-index-new) a-data)
              b-filename a-filename
              ]
          ;; (println b-filename)
          (if (.exists mgroup-b b-filename)
            (do
              (let [b-data (mapv #(u/gets % b-index-new) (.getKey mgroup-b b-filename))]
                ;; (io/delete-file b-filename true)
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
      ;; (println seq)
        (let [id (:id seq)
              b-filename (:d seq)
              b-data (mapv #(u/gets-format % b-index-new b-format) (read-csv-seq b-filename))]
          {:id id :d (mapv #(u/gets % write-index) (mapv add-nil b-data))}))
      (defn worker-func
        "refered in preview"
        [seq]
      ;; (println seq)
        (let [id (:id seq)
              b-filename (:d seq)
              b-data (mapv #(u/gets-format % b-index-new b-format) (.getKey mgroup-b b-filename))]
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
      :lifecycle/calls :clojask.join.outer-input/reader-calls}
     {:lifecycle/task :output
      :buffered-wtr/filename dist
      :lifecycle/calls :clojask.join.outer-output/writer-calls}]))

(def num-workers (atom 1))

;; (defn rem0?
;;   [event old-segment new-segment all-new-segment]
;;   ;; (spit "resources/debug.txt" (str new-segment "\n") :append true)
;;   (= (mod (:id new-segment) (deref num-workers)) 0))

;; (defn rem1?
;;   [event old-segment new-segment all-new-segment]
;;   (= (mod (:id new-segment) (deref num-workers)) 1))

;; (defn rem2?
;;   [event old-segment new-segment all-new-segment]
;;   (= (mod (:id new-segment) (deref num-workers)) 2))

;; (defn rem3?
;;   [event old-segment new-segment all-new-segment]
;;   (= (mod (:id new-segment) (deref num-workers)) 3))

;; (defn rem4?
;;   [event old-segment new-segment all-new-segment]
;;   (= (mod (:id new-segment) (deref num-workers)) 4))

;; (defn rem5?
;;   [event old-segment new-segment all-new-segment]
;;   (= (mod (:id new-segment) (deref num-workers)) 5))

;; (defn rem6?
;;   [event old-segment new-segment all-new-segment]
;;   (= (mod (:id new-segment) (deref num-workers)) 6))

;; (defn rem7?
;;   [event old-segment new-segment all-new-segment]
;;   (= (mod (:id new-segment) (deref num-workers)) 7))

;; (defn rem8?
;;   [event old-segment new-segment all-new-segment]
;;   (= (mod (:id new-segment) (deref num-workers)) 8))


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
    
  ;; (println flow-conditions) ;; !! debugging
  )

(defn config-env
  []
  (def env-config
    {:zookeeper/address "127.0.0.1:2188"
     :zookeeper/server? true
     :zookeeper.server/port 2188
     :onyx/tenancy-id id
     :onyx.log/file ".clojask/clojask.log"})

  (def peer-config
    {:zookeeper/address "127.0.0.1:2188"
     :onyx/tenancy-id id
     :onyx.peer/job-scheduler :onyx.job-scheduler/balanced
     :onyx.messaging/impl :aeron
     :onyx.messaging/peer-port 40200
     :onyx.messaging/bind-addr "localhost"
     :onyx.log/file ".clojask/clojask.log"})

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

(defn start-onyx-outer
  "start the onyx cluster with the specification inside dataframe"
  [num-work batch-size a b mgroup-a mgroup-b dist exception a-index b-index a-format b-format write-index output]
  ;; step 1
  (try
    (workflow-gen num-work)
    (config-env)
    (worker-func-gen a b mgroup-a mgroup-b exception a-index b-index a-format b-format write-index) ;;need some work
    (catalog-gen num-work batch-size)
    (lifecycle-gen "./.clojask/join/a" dist)
    (flow-cond-gen num-work)
    (input/inject-dataframe mgroup-a mgroup-b)
    (output/inject-write-func output)
    (catch Exception e (do
                         (shutdown)
                         (throw (ExecutionException. (format "[preparing stage (outer join)]  Refer to .clojask/clojask.log for detailed information. (original error: %s)" (.getMessage e)))))))
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
                         (throw (ExecutionException. (format "[submit-to-onyx stage (outer join)]  Refer to .clojask/clojask.log for detailed information. (original error: %s)" (.getMessage e)))))))

  ;; step 2
  (try
    (if (not= mgroup-b nil) (.final mgroup-b))
    (worker-func-gen2 a b mgroup-a mgroup-b exception a-index b-index a-format b-format write-index) ;;need some work
    (lifecycle-gen "./.clojask/join/b" dist)
    (input/inject-dataframe mgroup-b nil)

    (catch Exception e (do
                         (shutdown)
                         (throw (ExecutionException. (format "[preparing stage (outer join 2)]  Refer to .clojask/clojask.log for detailed information. (original error: %s)" (.getMessage e)))))))
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
                         (throw (ExecutionException. (format "[submit-to-onyx stage (outer join 2)]  Refer to .clojask/clojask.log for detailed information. (original error: %s)" (.getMessage e)))))))

  (try
    (shutdown)
    (catch Exception e (throw (ExecutionException. (format "[terminate-node stage (outer join)]  Refer to .clojask/clojask.log for detailed information. (original error: %s)" (.getMessage e))))))
  "success")
