(ns clojask.onyx-comps
  (:require [clojure.set :as set]
            [clojask.clojask-input :as input]
            [clojask.clojask-output :as output]
            [clojask.clojask-groupby :as groupby]
            [clojask.clojask-aggre :as aggre]
            [clojask.clojask-join :as join]
            [onyx.api :refer :all]
            [clojure.string :as string]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            [tech.v3.dataset :as ds]
            [clojure.data.csv :as csv]
            [clojask.utils :refer [eval-res eval-res-ne filter-check]]
            [clojask.join :refer [defn-join]])
  (:import (java.io BufferedReader FileReader BufferedWriter FileWriter)))


;; sample workflow
;;
;; [[:in :sample-worker1]
;;  [:in :sample-worker2]
;;  [:sample-worker1 :output]
;;  [:sample-worker2 :output]]

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

  ;; (println workflow) ; !!debugging
  )


;; (defn sample-worker
;;   [segment]
;;   ;; (println segment)
;;   (:id segment)
;;   ;; (update-in segment [:map] (fn [n] (assoc n :first (:id segment))))
;;   )

(def dataframe (atom nil))

;; the body of the worker function
;; (defn worker-body
;;   [seg]
;;   (let [df (deref dataframe)]
;;     (zipmap keys )))

(defn worker-func-gen
  [df exception index]
  (reset! dataframe df)
  (let [operations (.getDesc (:col-info (deref dataframe)))
        types (.getType (:col-info (deref dataframe)))
        filters (.getFilters (:row-info df))
        indices index]
    ;; (println indices)
    (if exception
      (defn worker-func
        [seg]
        (let [id (:id seg)
              data (string/split (:d seg) #"," -1)] ;; -1 is very important here!
        ;; (doseq [index (take (count operations) (iterate inc 0))]
        ;;   )
        ;; (spit "resources/debug.txt" (str seg "\n") :append true)
        ;; (spit "resources/debug.txt" (str types) :append true)
        ;; (spit "resources/debug.txt" (str operations) :append true)
        ;; (spit "resources/debug.txt" index :append true)
          (if (filter-check filters types data)
            {:id id :d (mapv (fn [_] (eval-res data types operations _)) indices)}
            {:id id})))
      (defn worker-func
        [seg]
        (let [id (:id seg)
              data (string/split (:d seg) #"," -1)]
          (if (filter-check filters types data)
            {:id id :d (mapv (fn [_] (eval-res-ne data types operations _)) indices)}
            {:id id})))))
  )

(defn catalog-gen
  "Generate the catalog for running Onyx"
  [num-work batch-size]
  ;; initialisation
  (def catalog [])

  ;; input
  (def catalog 
    (conj catalog
     {:onyx/name :in
      :onyx/plugin :clojask.clojask-input/input
      :onyx/type :input
      :onyx/medium :seq
      :seq/checkpoint? true
      :onyx/batch-size batch-size
      :onyx/max-peers 1
      :input/doc "Reads segments from a core.async channel"}))

    ;; for loop for sample workers
    (doseq [x (range 1 (+ num-work 1))]
      (let [worker-name (keyword (str "sample-worker" x))
            worker-function (keyword "clojask.onyx-comps" "worker-func")]
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
        :onyx/plugin :clojask.clojask-output/output
        :onyx/type :output
        :onyx/medium :core.async  ;; this is maked up
        :onyx/max-peers 1
        :onyx/batch-size batch-size
        :output/doc "Writes segments to the file"}))

    ;; (println catalog) ;; !! debugging
    )

(defn catalog-aggre-gen
  "Generate the catalog for running Onyx"
  [num-work batch-size]
  ;; initialisation
  (def catalog [])

  ;; input
  (def catalog
    (conj catalog
          {:onyx/name :in
           :onyx/plugin :clojask.clojask-input/input
           :onyx/type :input
           :onyx/medium :seq
           :seq/checkpoint? true
           :onyx/batch-size batch-size
           :onyx/max-peers 1
           :input/doc "Reads segments from a core.async channel"}))

    ;; for loop for sample workers
  (doseq [x (range 1 (+ num-work 1))]
    (let [worker-name (keyword (str "sample-worker" x))
          worker-function (keyword "clojask.onyx-comps" "worker-func")]
      (def catalog
        (conj catalog
              {:onyx/name worker-name
               :onyx/fn worker-function
               :onyx/type :function
               :onyx/batch-size batch-size
               :worker/doc "This is a worker node"}))))

    ;; output
  (def catalog
    (conj catalog
          {:onyx/name :output
           :onyx/plugin :clojask.clojask-aggre/output
           :onyx/type :output
           :onyx/medium :core.async  ;; this is maked up
           :onyx/max-peers 1
           :onyx/batch-size batch-size
           :output/doc "Writes segments to the file"}))

    ;; (println catalog) ;; !! debugging
  )

(defn catalog-groupby-gen
  "Generate the catalog for running Onyx"
  [num-work batch-size]
  ;; initialisation
  (def catalog [])

  ;; input
  (def catalog
    (conj catalog
          {:onyx/name :in
           :onyx/plugin :clojask.clojask-input/input
           :onyx/type :input
           :onyx/medium :seq
           :seq/checkpoint? true
           :onyx/batch-size batch-size
           :onyx/max-peers 1
           :input/doc "Reads segments from a core.async channel"}))

    ;; for loop for sample workers
  (doseq [x (range 1 (+ num-work 1))]
    (let [worker-name (keyword (str "sample-worker" x))
          worker-function (keyword "clojask.onyx-comps" "worker-func")]
      (def catalog
        (conj catalog
              {:onyx/name worker-name
               :onyx/fn worker-function
               :onyx/type :function
               :onyx/batch-size batch-size
               :worker/doc "This is a worker node"}))))

    ;; output
  (def catalog
    (conj catalog
          {:onyx/name :output
           :onyx/plugin :clojask.clojask-groupby/groupby
           :onyx/type :output
           :onyx/medium :core.async  ;; this is maked up
           :onyx/max-peers 1
           :onyx/batch-size batch-size
           :output/doc "Writes segments to the file"}))

    ;; (println catalog) ;; !! debugging
  )

(defn catalog-join-gen
  "Generate the catalog for running Onyx"
  [num-work batch-size]
  ;; initialisation
  (def catalog [])

  ;; input
  (def catalog
    (conj catalog
          {:onyx/name :in
           :onyx/plugin :clojask.clojask-input/input
           :onyx/type :input
           :onyx/medium :seq
           :seq/checkpoint? true
           :onyx/batch-size batch-size
           :onyx/max-peers 1
           :input/doc "Reads segments from a core.async channel"}))

    ;; for loop for sample workers
  (doseq [x (range 1 (+ num-work 1))]
    (let [worker-name (keyword (str "sample-worker" x))
          worker-function (keyword "clojask.onyx-comps" "worker-func")]
      (def catalog
        (conj catalog
              {:onyx/name worker-name
               :onyx/fn worker-function
               :onyx/type :function
               :onyx/batch-size batch-size
               :worker/doc "This is a worker node"}))))

    ;; output
  (def catalog
    (conj catalog
          {:onyx/name :output
           :onyx/plugin :clojask.clojask-join/join
           :onyx/type :output
           :onyx/medium :core.async  ;; this is maked up
           :onyx/max-peers 1
           :onyx/batch-size batch-size
           :output/doc "Writes segments to the file"}))

    ;; (println catalog) ;; !! debugging
  )


(defn inject-in-reader [event lifecycle]
  (let [rdr (FileReader. (:buffered-reader/filename lifecycle))
        ;; csv-data (csv/read-csv (BufferedReader. rdr))
        ]
    {:seq/rdr rdr
    ;;  :seq/seq (map zipmap ;; make the first row as headers and the following rows as values in a map structure e.g. {:tic AAPL} 
    ;;                (->> (first csv-data) ;; take the first row of the csv-data
    ;;                     (cons "clojask-id")
    ;;                     (map keyword) ;; make the header be the "key" in the map 
    ;;                     repeat)      ;; repeat the process for all the headers
    ;;                (map cons (iterate inc 1) (rest csv-data)))
    ;;  :seq/filters (:clojask/filters lifecycle)
    ;;  :seq/types (:clojask/types lifecycle)
     }))

(defn close-reader [event lifecycle]
  (.close (:seq/rdr event)))

;; (defn inject-out-writer [event lifecycle]
;;   (let [wrt (BufferedWriter. (FileWriter. (:buffered-writer/filename lifecycle)))]
;;     {:seq/wrt wrt}))

;; (defn close-writer [event lifecycle]
;;   (.close (:clojask/wtr event)))

;; (def writer-calls
;;   {:lifecycle/before-task-start inject-out-writer
;;    :lifecycle/after-task-stop close-writer})

(def in-calls
  {:lifecycle/before-task-start inject-in-reader
   :lifecycle/after-task-stop close-reader})


(defn lifecycle-gen
  [source dist order]
  (def lifecycles
    [{:lifecycle/task :in
      :buffered-reader/filename source
      ;; :clojask/filters (.getFilters (:row-info (deref dataframe)))
      ;; :clojask/types (.getType (:col-info (deref dataframe)))
      :lifecycle/calls ::in-calls}
     {:lifecycle/task :in
      :lifecycle/calls :clojask.clojask-input/reader-calls}
     {:lifecycle/task :output
      :buffered-wtr/filename dist
      :order order
      :lifecycle/calls :clojask.clojask-output/writer-calls}]))

(defn lifecycle-aggre-gen
  [source dist]
  (def lifecycles
    [{:lifecycle/task :in
      :buffered-reader/filename source
      ;; :clojask/filters (.getFilters (:row-info (deref dataframe)))
      ;; :clojask/types (.getType (:col-info (deref dataframe)))
      :lifecycle/calls ::in-calls}
     {:lifecycle/task :in
      :lifecycle/calls :clojask.clojask-input/reader-calls}
     {:lifecycle/task :output
      :buffered-wtr/filename dist
      ;; :order order
      :lifecycle/calls :clojask.clojask-aggre/writer-calls}]))

(defn lifecycle-groupby-gen
  [source dist keys key-index]
  (def lifecycles
    [{:lifecycle/task :in
      :buffered-reader/filename source
      :lifecycle/calls ::in-calls}
     {:lifecycle/task :in
      :lifecycle/calls :clojask.clojask-input/reader-calls}
     {:lifecycle/task :output
      :buffered-wtr/filename dist
      ;; :clojask/groupby-keys keys
      :clojask/key-index key-index
      :lifecycle/calls :clojask.clojask-groupby/writer-aggre-calls}]))

(defn lifecycle-join-gen
  [source dist a b a-keys b-keys a-roll b-roll join-type]
  (def lifecycles
    [{:lifecycle/task :in
      :buffered-reader/filename source
      :lifecycle/calls ::in-calls}
     {:lifecycle/task :in
      :lifecycle/calls :clojask.clojask-input/reader-calls}
     {:lifecycle/task :output
      :buffered-wtr/filename dist
      ;; :clojask/a-keys a-keys
      ;; :clojask/b-keys b-keys 
      :clojask/a-roll a-roll
      :clojask/b-roll b-roll
      :clojask/a-map (.getKeyIndex (.col-info a)) 
      :clojask/b-map (.getKeyIndex (.col-info b))
      :clojask/join-type join-type
      :lifecycle/calls :clojask.clojask-join/writer-join-calls}]))

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
          predicate-function (keyword "clojask.onyx-comps" (str "rem" (- x 1) "?"))]
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

(defn start-onyx
  "start the onyx cluster with the specification inside dataframe"
  [num-work batch-size dataframe dist exception order index]
  (try
    (workflow-gen num-work)
    (config-env)
    (worker-func-gen dataframe exception index) ;;need some work
    (catalog-gen num-work batch-size)
    (lifecycle-gen (.path dataframe) dist order)
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

(defn start-onyx-aggre-only
  "start the onyx cluster with the specification inside dataframe"
  [num-work batch-size dataframe dist exception aggre-func index select]
  (try
    (workflow-gen num-work)
    (config-env)
    (worker-func-gen dataframe exception index) ;;need some work
    (catalog-aggre-gen num-work batch-size)
    (lifecycle-aggre-gen (.path dataframe) dist)
    (flow-cond-gen num-work)
    (input/inject-dataframe dataframe)
    (aggre/inject-dataframe dataframe aggre-func select)
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
                         (throw (Exception. (str "[submit-to-onyx stage (aggregate)] " (.getMessage e)))))))
  (try
    (shutdown)
    (catch Exception e (throw (Exception. (str "[terminate-node stage (aggregate)] " (.getMessage e))))))
  "success")

(defn start-onyx-groupby
  "start the onyx cluster with the specification inside dataframe"
  [num-work batch-size dataframe dist groupby-keys groupby-index exception]
  ;; (println groupby-index)
  (try
    (workflow-gen num-work)
    (config-env)
    (worker-func-gen dataframe exception groupby-index) ;;need some work
    (catalog-groupby-gen num-work batch-size)
    (lifecycle-groupby-gen (.path dataframe) dist groupby-keys (.getKeyIndex (.col-info dataframe)))
    (flow-cond-gen num-work)
    (input/inject-dataframe dataframe)
    (groupby/inject-dataframe dataframe groupby-keys)
    (catch Exception e (throw (Exception. (str "[preparing stage (group by)] " (.getMessage e))))))
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
                         (throw (Exception. (str "[submit-to-onyx stage (group by)] " (.getMessage e)))))))
  (try
    (shutdown)
    (catch Exception e (throw (Exception. (str "[terminate-node stage (group by)] " (.getMessage e))))))
  "success")

(defn start-onyx-join
  "start the onyx cluster with the specification inside dataframe"
  [num-work batch-size dataframe b dist exception a-keys b-keys a-roll b-roll join-type & [limit]]
  ;; dataframe means a
  (try
    (workflow-gen num-work)
    (config-env)
    (worker-func-gen dataframe exception (take (count (.getKeyIndex (:col-info dataframe))) (iterate inc 0))) ;;need some work
    (catalog-join-gen num-work batch-size)
    (lifecycle-join-gen (.path dataframe) dist dataframe b a-keys b-keys a-roll b-roll join-type)
    (flow-cond-gen num-work)
    (input/inject-dataframe dataframe)
    (join/inject-dataframe dataframe b a-keys b-keys)
    (let [limit (or limit (fn [a b] true))]
     (defn-join join-type limit))
    (catch Exception e (throw (Exception. (str "[preparing stage (join)] " (.getMessage e))))))
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
                         (throw (Exception. (str "[submit-to-onyx stage (join)] " (.getMessage e)))))))
  (try
    (shutdown)
    (catch Exception e (throw (Exception. (str "[terminate-node stage (join)] " (.getMessage e))))))
  "success")


;; !! debugging
(defn -main
  [& args]
  ;; (catalog-gen 2 10)
  ;; (workflow-gen 2)
  ;; (flow-cond-gen 2)
  ;; (start-onyx 2 10 )
  )