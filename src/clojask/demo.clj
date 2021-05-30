(ns clojask.demo
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [onyx.extensions :as extensions]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.plugin.seq :refer :all]
            [onyx.api]
            [onyx.test-helper :refer [with-test-env feedback-exception!]]
            [tech.v3.dataset :as ds]
            [clojure.java.io :refer [resource]])
  (:import (java.io BufferedReader FileReader BufferedWriter FileWriter))
  (:gen-class))

;; (defn spit2 [file-name data]
;;   (with-open [wtr (BufferedWriter. (FileWriter.	file-name))]
;;     (.write wtr	data)))

(def wtr (BufferedWriter. (FileWriter.	"resources/test.csv")))
;; (.write wtr "asd")

(defn sample-worker
  [segment]
  ;; (println segment)
  segment
  ;; (update-in segment [:map] (fn [n] (assoc n :first (:id segment))))
  )

(defn output
  [segment]
  (.write wtr (str segment "\n"))
  nil)

;;                 a vector of map
;;                        |
;;                    sample-worker
;;                        |
;;                      output
;;                        |
;;                       end
;;                     
;; 
;; 

(def workflow
  [[:in :sample-worker]
   [:sample-worker :output]
   [:output :end]])

;;; Use core.async for I/O
(def capacity 1000)

(def input-chan (chan capacity))
(def input-buffer (atom {}))

(def output-chan (chan capacity))

(def batch-size 10)

(def catalog
  [{:onyx/name :in
    ;; :onyx/plugin :onyx.plugin.core-async/input
    :onyx/plugin :onyx.plugin.seq/input
    :onyx/type :input
    ;; :onyx/medium :core.async
    :onyx/medium :seq
    :seq/checkpoint? true

    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :sample-worker
    ;; :demo/writer wtr
    ;; :onyx/params [:demo/writer]
    :onyx/fn :clojask.demo/sample-worker
    :onyx/type :function
    :onyx/batch-size batch-size
    :onyx/doc "do nothing"}

   {:onyx/name :output
    :onyx/fn :clojask.demo/output
    :onyx/type :function
    :onyx/max-peers 1
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to the file"}
   
   {:onyx/name :end
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/max-peers 1
    :onyx/batch-size batch-size
    :onyx/doc "Dummy end node"}])

;; (def windows
;;   [{:window/id :word-counter
;;     :window/task :count-words
;;     :window/type :global
;;     :window/aggregation :onyx.windowing.aggregation/count}])

;; (def triggers
;;   [{:trigger/window-id :word-counter
;;     :trigger/id :sync
;;     :trigger/on :onyx.triggers/segment
;;     :trigger/threshold [5 :elements]
;;     :trigger/sync ::dump-window!}])

;; (defn dump-window!
;;   "Operations when one job is done."
;;   [event window trigger {:keys [group-key] :as opts} state]
;;   ;; (println event window trigger opts)
;;   (println group-key "->" state))


(def id (java.util.UUID/randomUUID))

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

(def v-peers (onyx.api/start-peers n-peers peer-group))

;; (defn inject-in-ch [event lifecycle]
;;   {:core.async/buffer input-buffer
;;    :core.async/chan input-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan output-chan})

;; (def in-calls
;;   {:lifecycle/before-task-start inject-in-ch})
(defn inject-in-reader [event lifecycle]
  (let [rdr (FileReader. (:buffered-reader/filename lifecycle))]
    {:seq/rdr rdr
     :seq/seq (line-seq (BufferedReader. rdr))}))

(defn close-reader [event lifecycle]
  (.close (:seq/rdr event)))

;; (defn inject-out-writer [event lifecycle]
;;   (let [wrt (FileWritter. (:buffered-writer/filename lifecycle))]
;;     {:seq/wrt wrt}))

(def in-calls
  {:lifecycle/before-task-start inject-in-reader
   :lifecycle/after-task-stop close-reader})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})
;; (def out-calls
;;   {:lifecycle/before-task-start inject-out-writer
;;    :lifecycle/after-task-stop })

(def lifecycles
  ;; [{:lifecycle/task :in
  ;;   :lifecycle/calls :clojask.demo/in-calls
  ;;   :core.async/id (java.util.UUID/randomUUID)}
  ;;  {:lifecycle/task :in
  ;;   :lifecycle/calls :onyx.plugin.core-async/reader-calls}
  [{:lifecycle/task :in
    :buffered-reader/filename "resources/CRSP-extract.csv"
    :lifecycle/calls ::in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.seq/reader-calls}
  ;;  {:lifecycle/task :first-half
  ;;   :buffer-writer wtr
  ;;   :lifecycle/calls ::first-half-calls}
   {:lifecycle/task :end
    :lifecycle/calls :clojask.demo/out-calls
    :core.async/id (java.util.UUID/randomUUID)}
   ])

;; (def flow-conditions
;;   [{:flow/from :in
;;     :flow/to [:first-half]
;;     :flow/predicate :clojask.demo/first-half?
;;     :flow/doc ""}
;;    {:flow/from :in
;;     :flow/to [:second-half]
;;     :flow/predicate :clojask.demo/second-half?
;;     :flow/doc ""}])

;; (defn collect-outputs
;;   "Collects the output from the output channel"
;;   []
;;   (map #(take-segments! % 50) [output-chan]))

(def ONYX true)

(defn -main
  [& args]
  (if ONYX
    (do
      ;; (prepare-input)
      (let [submission (onyx.api/submit-job peer-config
                                            {:workflow workflow
                                             :catalog catalog
                                             :lifecycles lifecycles
                                            ;;  :flow-conditions flow-conditions
                                             :task-scheduler :onyx.task-scheduler/balanced})
            job-id (:job-id submission)]
        (println submission)
        (assert job-id "Job was not successfully submitted")
        (feedback-exception! peer-config job-id))
      (doseq [v-peer v-peers]
        (onyx.api/shutdown-peer v-peer))
      (println 4)
      (onyx.api/shutdown-peer-group peer-group)
      (println 5)
      (onyx.api/shutdown-env env)))
  (def dataset (ds/->dataset "resources/CRSP-extract.csv"))
;;   (println dataset)
  (println (ds/head dataset)))