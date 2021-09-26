(ns clojask.core-test
    (:require [clojure.test :refer :all]
              [clojask.DataFrame :refer :all]
              [clojask.utils :refer :all]
              [clojask.groupby :refer :all]
              [clojask.sort :refer :all]))

;; !! TO-DO: change csv file path to load from GitHub

(deftest df-api-test
  (testing "Single dataframe manipulation APIs"
    (def y (dataframe "resources/Employees-large.csv" :have-col true))
    (is (= clojask.DataFrame.DataFrame (type y)))
    (is (= clojask.DataFrame.DataFrame (type (set-type y "Salary" "double"))))
    (is (= clojask.DataFrame.DataFrame (type (set-parser y "Department" #(Double/parseDouble %)))))
    (is (= clojask.DataFrame.DataFrame (type (filter y "Salary" (fn [salary] (<= salary 800))))))
    (is (= clojask.DataFrame.DataFrame (type (operate y - "Salary"))))
    (is (= clojask.DataFrame.DataFrame (type (operate y str ["Employee" "Salary"] "new-col"))))
    (is (= clojask.DataFrame.DataFrame (type (group-by y ["Department"]))))
    (is (= clojask.DataFrame.DataFrame (type (aggregate y min ["Employee"] ["new-employee"]))))
    (is (= "success" (compute y 8 "resources/test.csv" :exception true)))
    ))

(deftest col-api-test
    (testing "Column manipulation APIs"
    (def y (dataframe "resources/Employees-large.csv" :have-col true))
    (reorder-col y ["Employee" "Department" "EmployeeName" "Salary"])
    (is (= (.getKeys (.col-info y)) ["Employee" "Department" "EmployeeName" "Salary"]))
    (rename-col y ["Employee" "new-Department" "EmployeeName" "Salary"])
    (is (= (.getKeys (.col-info y)) ["Employee" "new-Department" "EmployeeName" "Salary"]))
    ))

(deftest join-api-test
    (testing "Join dataframes APIs"
    (def x (dataframe "resources/Employees-large.csv"))
    (def y (dataframe "resources/Employees.csv"))
    (is (= "success" (left-join x y ["Employee"] ["Employee"] 4 "resources/test.csv" :exception false)))
    (is (= "success" (right-join x y ["Employee"] ["Employee"] 4 "resources/test.csv" :exception false)))
    (is (= "success" (inner-join x y ["Employee"] ["Employee"] 4 "resources/test.csv" :exception false)))
    ))


;; (deftest dataset-test
;;   (def dataset (ds/->dataset "https://github.com/techascent/tech.ml.dataset/raw/master/test/data/stocks.csv"))
;;   (println (ds/head dataset)))

;; ;;; Word count!

;; ;;; Split a sentence into words, emit a seq of segments
;; (defn split-sentence [{:keys [id sentence]}]
;;   (map-indexed
;;    (fn [i word]
;;      ;; Use the originally unique key, compounded
;;      ;; with another key that we'll spin up from this
;;      ;; particular sentence to make it globally unique.
;;      {:id (str id "-" i)
;;       :word word})
;;    (clojure.string/split sentence #"\s+")))

;; (def workflow
;;   [[:in :split-sentence]
;;    [:split-sentence :count-words]
;;    [:count-words :out]])

;; ;;; Use core.async for I/O
;; (def capacity 1000)

;; (def input-chan (chan capacity))
;; (def input-buffer (atom {}))

;; (def output-chan (chan capacity))

;; (def batch-size 10)

;; (def catalog
;;   [{:onyx/name :in
;;     :onyx/plugin :onyx.plugin.core-async/input
;;     :onyx/type :input
;;     :onyx/medium :core.async
;;     :onyx/batch-size batch-size
;;     :onyx/max-peers 1
;;     :onyx/doc "Reads segments from a core.async channel"}

;;    {:onyx/name :split-sentence
;;     :onyx/fn :aggregation.core/split-sentence
;;     :onyx/type :function
;;     :onyx/batch-size batch-size}

;;    {:onyx/name :count-words
;;     :onyx/fn :clojure.core/identity
;;     :onyx/type :function
;;     :onyx/group-by-key :word
;;     :onyx/flux-policy :kill
;;     :onyx/min-peers 1
;;     :onyx/batch-size 1000}

;;    {:onyx/name :out
;;     :onyx/plugin :onyx.plugin.core-async/output
;;     :onyx/type :output
;;     :onyx/medium :core.async
;;     :onyx/max-peers 1
;;     :onyx/batch-size batch-size
;;     :onyx/doc "Writes segments to a core.async channel"}])

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
;;   [event window trigger {:keys [group-key] :as opts} state]
;;   (println group-key "->" state))

;; ;; Seriously, my coffee's gone cold. :/
;; (def input-segments
;;   [{:id 0 :event-time 0 :sentence "My name is Mike"}
;;    {:id 1 :event-time 0 :sentence "My coffee's gone cold"}
;;    {:id 2 :event-time 0 :sentence "Time to get a new cup"}
;;    {:id 3 :event-time 0 :sentence "Coffee coffee coffee"}
;;    {:id 4 :event-time 0 :sentence "Om nom nom nom"}])

;; (doseq [segment input-segments]
;;   (>!! input-chan segment))

;; ;; The core.async channel to be closed when using batch mode,
;; ;; otherwise an Onyx peer will block indefinitely trying to read.
;; (close! input-chan)

;; (def id (java.util.UUID/randomUUID))

;; (def env-config
;;   {:zookeeper/address "127.0.0.1:2188"
;;    :zookeeper/server? true
;;    :zookeeper.server/port 2188
;;    :onyx/tenancy-id id})

;; (def peer-config
;;   {:zookeeper/address "127.0.0.1:2188"
;;    :onyx/tenancy-id id
;;    :onyx.peer/job-scheduler :onyx.job-scheduler/balanced
;;    :onyx.messaging/impl :aeron
;;    :onyx.messaging/peer-port 40200
;;    :onyx.messaging/bind-addr "localhost"})

;; (def env (onyx.api/start-env env-config))

;; (def peer-group (onyx.api/start-peer-group peer-config))

;; (def n-peers (count (set (mapcat identity workflow))))

;; (def v-peers (onyx.api/start-peers n-peers peer-group))

;; (defn inject-in-ch [event lifecycle]
;;   {:core.async/buffer input-buffer
;;    :core.async/chan input-chan})

;; (defn inject-out-ch [event lifecycle]
;;   {:core.async/chan output-chan})

;; (def in-calls
;;   {:lifecycle/before-task-start inject-in-ch})

;; (def out-calls
;;   {:lifecycle/before-task-start inject-out-ch})

;; (def lifecycles
;;   [{:lifecycle/task :in
;;     :lifecycle/calls :aggregation.core/in-calls}
;;    {:lifecycle/task :in
;;     :lifecycle/calls :onyx.plugin.core-async/reader-calls}
;;    {:lifecycle/task :out
;;     :lifecycle/calls :aggregation.core/out-calls}
;;    {:lifecycle/task :out
;;     :lifecycle/calls :onyx.plugin.core-async/writer-calls}])

;; (deftest onyx-test
;;   (let [submission (onyx.api/submit-job peer-config
;;                                         {:workflow workflow
;;                                          :catalog catalog
;;                                          :lifecycles lifecycles
;;                                          :windows windows
;;                                          :triggers triggers
;;                                          :task-scheduler :onyx.task-scheduler/balanced})]
;;     (println (onyx.api/await-job-completion peer-config (:job-id submission))))
;;   (clojure.pprint/pprint (onyx.plugin.core-async/take-segments! output-chan 50))
;;   ;; (clojure.pprint/pprint <!! output-chan)
;;   (doseq [v-peer v-peers]
;;     (onyx.api/shutdown-peer v-peer))

;;   (onyx.api/shutdown-peer-group peer-group)

;;   (onyx.api/shutdown-env env))
