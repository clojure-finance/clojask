(ns clojask.core
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [onyx.extensions :as extensions]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api]
            [tech.v3.dataset :as ds])
  (:gen-class))

;;; Split a sentence into words, emit a seq of segments
(defn split-sentence [{:keys [id sentence]}]
  (map-indexed
   (fn [i word]
     ;; Use the originally unique key, compounded
     ;; with another key that we'll spin up from this
     ;; particular sentence to make it globally unique.
     {:id (str id "-" i)
      :word word})
   (clojure.string/split sentence #"\s+")))

(def workflow
  [[:in :split-sentence]
   [:split-sentence :count-words]
   [:count-words :out]])

;;; Use core.async for I/O
(def capacity 1000)

(def input-chan (chan capacity))
(def input-buffer (atom {}))

(def output-chan (chan capacity))

(def batch-size 10)

(def catalog
  [{:onyx/name :in
    :onyx/plugin :onyx.plugin.core-async/input
    :onyx/type :input
    :onyx/medium :core.async
    :onyx/batch-size batch-size
    :onyx/max-peers 1
    :onyx/doc "Reads segments from a core.async channel"}

   {:onyx/name :split-sentence
    :onyx/fn :clojack.core/split-sentence
    :onyx/type :function
    :onyx/batch-size batch-size}

   {:onyx/name :count-words
    :onyx/fn :clojure.core/identity
    :onyx/type :function
    :onyx/group-by-key :word
    :onyx/flux-policy :kill
    :onyx/min-peers 1
    :onyx/batch-size 1000}

   {:onyx/name :out
    :onyx/plugin :onyx.plugin.core-async/output
    :onyx/type :output
    :onyx/medium :core.async
    :onyx/max-peers 1
    :onyx/batch-size batch-size
    :onyx/doc "Writes segments to a core.async channel"}])

(def windows
  [{:window/id :word-counter
    :window/task :count-words
    :window/type :global
    :window/aggregation :onyx.windowing.aggregation/count}])

(def triggers
  [{:trigger/window-id :word-counter
    :trigger/id :sync
    :trigger/on :onyx.triggers/segment
    :trigger/threshold [5 :elements]
    :trigger/sync ::dump-window!}])

(defn dump-window!
  "Operations when one job is done."
  [event window trigger {:keys [group-key] :as opts} state]
  ;; (println event window trigger opts)
  (println group-key "->" state))

;; Seriously, my coffee's gone cold. :/
(def input-segments
  [{:id 0 :event-time 0 :sentence "My name is Mike"}
   {:id 1 :event-time 0 :sentence "My coffee's gone cold"}
   {:id 2 :event-time 0 :sentence "Time to get a new cup"}
   {:id 3 :event-time 0 :sentence "Coffee coffee coffee"}
   {:id 4 :event-time 0 :sentence "Om nom nom nom"}])

(doseq [segment input-segments]
  (>!! input-chan segment))

;; The core.async channel to be closed when using batch mode,
;; otherwise an Onyx peer will block indefinitely trying to read.
(close! input-chan)

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

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(def lifecycles
  [{:lifecycle/task :in
    :lifecycle/calls :clojack.core/in-calls}
   {:lifecycle/task :in
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}
   {:lifecycle/task :out
    :lifecycle/calls :clojack.core/out-calls}
   {:lifecycle/task :out
    :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

(defn -main
  [& args]
  (println n-peers)
  (let [submission (onyx.api/submit-job peer-config
                                        {:workflow workflow
                                         :catalog catalog
                                         :lifecycles lifecycles
                                         :windows windows
                                         :triggers triggers
                                         :task-scheduler :onyx.task-scheduler/balanced})]
    (println 1)
    (onyx.api/await-job-completion peer-config (:job-id submission)))
  (println 2)
  (onyx.plugin.core-async/take-segments! output-chan 50)
  (println 3)
  (doseq [v-peer v-peers]
    (onyx.api/shutdown-peer v-peer))
  (println 4)
  (onyx.api/shutdown-peer-group peer-group)
  (println 5)
  (onyx.api/shutdown-env env))