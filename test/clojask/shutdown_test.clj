(ns shutdown-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojask.onyx-comps :as oc]
            [clojask.aggregate.aggre-onyx-comps :as ag]
            [clojask.dataframe :as ck])
  (:import [com.clojask.exception ExecutionException]))

(def input "test/clojask/Employees-example.csv")

(defn- env-is-down []
  (is (nil? oc/env))
  (is (nil? oc/peer-group))
  (is (nil? oc/v-peers)))

(defn- compute-still-works []
  (let [out "test/clojask/test_outputs/shutdown-test.csv"
        df (ck/dataframe input)]
    (io/make-parents out)
    (ck/compute df 2 out)
    (is (= 8 (count (line-seq (io/reader out)))))))

(deftest partial-startup-releases-zookeeper
  (testing "A startup that fails after ZooKeeper is up can be shut down, and later computes still work"
    (oc/workflow-gen 1)
    ;; Force the peer group to fail after the env (embedded ZooKeeper) is up.
    (with-redefs [onyx.api/start-peer-group
                  (fn [_] (throw (RuntimeException. "simulated peer-group failure")))]
      (is (thrown? RuntimeException (oc/config-env))))
    (is (some? oc/env) "env was started before the failure")
    (is (nil? oc/peer-group) "peer group never started")
    ;; Must not throw, and must release ZooKeeper's port.
    (oc/shutdown)
    (is (nil? oc/env))
    ;; A real compute in the same JVM must now succeed.
    (compute-still-works)))

(deftest failed-job-preparation-releases-environment
  (testing "with-onyx-env tears the environment down when a job's preparation fails"
    (is (thrown-with-msg? ExecutionException #"preparing stage \(probe\)"
                          (oc/with-onyx-env "probe" 1
                            (fn [] (oc/run-job "probe"
                                               (fn [] (throw (RuntimeException. "simulated prepare failure"))))))))
    (env-is-down)))

(deftest failed-aggregate-preparation-releases-environment
  (testing "The group-by aggregate driver shares that handling"
    (let [df (ck/dataframe input)]
      (with-redefs [ag/worker-func-gen (fn [& _] (throw (RuntimeException. "simulated prepare failure")))]
        (is (thrown-with-msg? ExecutionException #"preparing stage \(groupby aggregate\)"
                              (ag/start-onyx-aggre 1 10 df nil "test/clojask/test_outputs/never.csv" false [] [] {} nil))))
      (env-is-down)
      (compute-still-works))))
