(ns clojask.onyx-comps
  (:require [clojask.clojask-input :refer :all]
            [clojask.clojask-output :refer :all]
            [onyx.api :refer :all]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            [tech.v3.dataset :as ds]
            [clojure.data.csv :as csv])
  (:import (java.io BufferedReader FileReader BufferedWriter FileWriter)))


;; [[:in :sample-worker1]
;;  [:in :sample-worker2]
;;  [:sample-worker1 :output]
;;  [:sample-worker2 :output]]
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

  (println workflow) ; !!debugging
  )


;; (defn sample-worker
;;   [segment]
;;   ;; (println segment)
;;   (:clojask-id segment)
;;   ;; (update-in segment [:map] (fn [n] (assoc n :first (:id segment))))
;;   )
(defn worker-func-gen
  [body]
  (defn worker-func
    [seg]
    body)
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
            worker-function (keyword "clojask.onyx-comps" (str "sample-worker" x))]
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

    (println catalog) ;; !!debugging
    )


(defn inject-in-reader [event lifecycle]
  (let [rdr (FileReader. (:buffered-reader/filename lifecycle))
        csv-data (csv/read-csv (BufferedReader. rdr))]
    {:seq/rdr rdr
     :seq/seq (map zipmap ;; make the first row as headers and the following rows as values in a map structure e.g. {:tic AAPL} 
                   (->> (first csv-data) ;; take the first row of the csv-data
                        (cons "clojask-id")
                        (map keyword) ;; make the header be the "key" in the map 
                        repeat)      ;; repeat the process for all the headers
                   (map cons (iterate inc 1) (rest csv-data)))}))

(defn close-reader [event lifecycle]
  (.close (:seq/rdr event)))

(defn inject-out-writer [event lifecycle]
  (let [wrt (BufferedWriter. (FileWriter. (:buffered-writer/filename lifecycle)))]
    {:seq/wrt wrt}))

(defn close-writer [event lifecycle]
  (.close (:clojask/wtr event)))

(def in-calls
  {:lifecycle/before-task-start inject-in-reader
   :lifecycle/after-task-stop close-reader})


(defn lifecycle-gen
  [source dist]
  (def lifecycles
    [{:lifecycle/task :in
      :buffered-reader/filename source
      :lifecycle/calls ::in-calls}
     {:lifecycle/task :in
      :lifecycle/calls :clojask.clojask-input/reader-calls}
     {:lifecycle/task :output
      :buffered-wtr/filename dist
      :lifecycle/calls ::writer-calls}]))

(def num-workers (atom 1))

(defn rem0?
  [event old-segment new-segment all-new-segment]
  (= (mod (:clojask-id new-segment) (deref num-workers)) 0))

(defn rem1?
  [event old-segment new-segment all-new-segment]
  (= (mod (:clojask-id new-segment) (deref num-workers)) 1))

(defn rem2?
  [event old-segment new-segment all-new-segment]
  (= (mod (:clojask-id new-segment) (deref num-workers)) 2))

(defn rem3?
  [event old-segment new-segment all-new-segment]
  (= (mod (:clojask-id new-segment) (deref num-workers)) 3))

(defn rem4?
  [event old-segment new-segment all-new-segment]
  (= (mod (:clojask-id new-segment) (deref num-workers)) 4))

(defn rem5?
  [event old-segment new-segment all-new-segment]
  (= (mod (:clojask-id new-segment) (deref num-workers)) 5))

(defn rem6?
  [event old-segment new-segment all-new-segment]
  (= (mod (:clojask-id new-segment) (deref num-workers)) 6))

(defn rem7?
  [event old-segment new-segment all-new-segment]
  (= (mod (:clojask-id new-segment) (deref num-workers)) 7))

(defn rem8?
  [event old-segment new-segment all-new-segment]
  (= (mod (:clojask-id new-segment) (deref num-workers)) 8))


;; [{:flow/from :in
;;   :flow/to [:sample-worker1]
;;   :flow/predicate :clojask.onyx-comps/rem0?
;;   :flow/doc ""}
;;  {:flow/from :in
;;   :flow/to [:sample-worker2]
;;   :flow/predicate :clojask.onyx-comps/rem1?
;;   :flow/doc ""}]
(defn flow-cond-gen
  [num-work]
  (set! num-workers num-work)
  (def flow-conditions []))

(def id (java.util.UUID/randomUUID))

(defn config-env
  []
  (def env-config
    {:zookeeper/address "127.0.0.1:2188"
     :zookeeper/server? true
     :zookeeper.server/port 2188
     :onyx/tenancy-id id})

  (def peer-config
    {:zookeeper/address "127.0.0.1:2188"
     :onyx/tenancy-id id
     :onyx.peer/job-scheduler :onyx.job-scheduler/balanced
     :onyx.messaging/impl :aeron
     :onyx.messaging/peer-port 40200
     :onyx.messaging/bind-addr "localhost"})


  (def env (onyx.api/start-env env-config))

  (def peer-group (onyx.api/start-peer-group peer-config))

  (def n-peers (count (set (mapcat identity workflow))))

  (def v-peers (onyx.api/start-peers n-peers peer-group)))

(defn start-onyx
  "start the onyx cluster with the specification inside dataframe"
  [num-work batch-size dataframe dist]
  (try
    (config-env)
    (workflow-gen num-work)
;;   (worker-func-gen dataframe) ;;need some work
    (catalog-gen num-work)
    (lifecycle-gen (.path dataframe) dist)
    (flow-cond-gen num-work)
    
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
    (catch Exception e (throw (Exception. (str "[submit-to-onyx stage] " (.getMessage e))))))
  (try
    (doseq [v-peer v-peers]
      (onyx.api/shutdown-peer v-peer))
    (onyx.api/shutdown-peer-group peer-group)
    (onyx.api/shutdown-env env)
    (catch Exception e (throw (Exception. (str "[terminate-node stage] " (.getMessage e)))))))

(defn -main
  [& args]
  (catalog-gen 2 10)
  (workflow-gen 2)
  )