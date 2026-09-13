(ns shutdown-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojask.onyx-comps :as oc]
            [clojask.dataframe :as ck]))

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
    (let [out "test/clojask/test_outputs/shutdown-test.csv"
          df (ck/dataframe "test/clojask/Employees-example.csv")]
      (io/make-parents out)
      (ck/compute df 2 out)
      (is (= 8 (count (line-seq (io/reader out))))))))
