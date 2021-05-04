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

(def wtr (BufferedWriter. (FileWriter.	"test.csv")))
;; (.write wtr "asd")
(defn first-half
  [segment]
  ;; (println segment)
  (.write wtr segment)
  ;; (update-in segment [:map] (fn [n] (assoc n :first (:id segment))))
  )

(defn second-half
  [segment]
  ;; (update-in segment [:map] (fn [n] (assoc n :second (:id segment))))
  )

;;                 a vector of map
;;                /              \
;;          first half         second half
;;     add a new key :first   add a new key :second    
;;           |                        |
;;         output1                 output2
;; 
;; 

(def workflow
  [[:in :first-half]
   [:in :second-half]
   [:first-half :output1]
   [:second-half :output2]])

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

   {:onyx/name :first-half
    ;; :demo/writer wtr
    ;; :onyx/params [:demo/writer]
    :onyx/fn :clojask.demo/first-half
    :onyx/type :function
    :onyx/batch-size batch-size
    :onyx/doc "Append key :first to the first half"}

   {:onyx/name :second-half
    :onyx/fn :clojask.demo/second-half
    :onyx/type :function
    :onyx/batch-size batch-size
    :onyx/doc "Append key :second to the second half"}

   {:onyx/name :output1
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/max-peers 1
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}

   {:onyx/name :output2
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/max-peers 1
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}])

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


(def input-segments
  [{:id 0 :map {:seg 1}}
   {:id 1 :map {:seg 2}}
   {:id 2 :map {:seg 3}}
   {:id 3 :map {:seg 4}}
   {:id 4 :map {:seg 5}}])

;; [{:id xxx :name xxx} {} {}]

;; {:column-name [] }

(defn prepare-input
  []
  (doseq [segment input-segments]
    (>!! input-chan segment))

  ;; The core.async channel to be closed when using batch mode,
  ;; otherwise an Onyx peer will block indefinitely trying to read.
  (close! input-chan))

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

(defn inject-in-ch [event lifecycle]
  {:core.async/buffer input-buffer
   :core.async/chan input-chan})

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

(def first-half-calls
  )

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
    :buffered-reader/filename "/Users/lyc/Desktop/RA clojure/data-sorted-cleaned/data-CRSP-sorted-cleaned.csv"
    :lifecycle/calls ::in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.seq/reader-calls}
  ;;  {:lifecycle/task :first-half
  ;;   :buffer-writer wtr
  ;;   :lifecycle/calls ::first-half-calls}
   {:lifecycle/task :output1
    :lifecycle/calls :clojask.demo/out-calls
    :core.async/id (java.util.UUID/randomUUID)}
   {:lifecycle/task :output1
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}
  ;;  {:lifecycle/task :output1
  ;;   :buffered-writer/filename "test.csv"
  ;;   :lifecycle/calls :clojask.demo/out-calls}
  ;;  {:lifecycle/task :output1
  ;;   :lifecycle/calls :onyx.plugin.core-async/writer-calls}
   {:lifecycle/task :output2
    :lifecycle/calls :clojask.demo/out-calls
    :core.async/id (java.util.UUID/randomUUID)}
   {:lifecycle/task :output2
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(defn first-half? [event old-segment new-segment all-new-segment]
  (< (:id new-segment) 3))

(defn second-half? [event old-segment new-segment all-new-segment]
  (>= (:id new-segment) 3))

;; (def flow-conditions
;;   [{:flow/from :in
;;     :flow/to [:first-half]
;;     :flow/predicate :clojask.demo/first-half?
;;     :flow/doc ""}
;;    {:flow/from :in
;;     :flow/to [:second-half]
;;     :flow/predicate :clojask.demo/second-half?
;;     :flow/doc ""}])

(defn collect-outputs
  "Collects the output from the output channel"
  []
  (map #(take-segments! % 50) [output-chan]))

(def ONYX true)

(defn -main
  [& args]
  (if ONYX
    (do
      (prepare-input)
      (let [submission (onyx.api/submit-job peer-config
                                            {:workflow workflow
                                             :catalog catalog
                                             :lifecycles lifecycles
                                            ;;  :flow-conditions flow-conditions
                                             :task-scheduler :onyx.task-scheduler/balanced})
            job-id (:job-id submission)]
        (println submission)
        (assert job-id "Job was not successfully submitted")
        (feedback-exception! peer-config job-id)
;;   (onyx.plugin.core-async/take-segments! output-chan 50)
        (let [output (collect-outputs)]
          (println output)))
      (doseq [v-peer v-peers]
        (onyx.api/shutdown-peer v-peer))
      (println 4)
      (onyx.api/shutdown-peer-group peer-group)
      (println 5)
      (onyx.api/shutdown-env env)))
  (def dataset (ds/->dataset "resources/CRSP-extract.csv"))
;;   (println dataset)
  (println (ds/head dataset)))