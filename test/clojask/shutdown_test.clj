(ns shutdown-test
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojask.onyx-comps :as oc]
            [clojask.aggregate.aggre-onyx-comps :as ag]
            [clojask.dataframe :as ck]
            [onyx.api]
            [taoensso.timbre :as timbre])
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

(deftest ports-and-directory-are-configurable
  (testing "system properties choose the ZooKeeper port, the Aeron port and the Aeron directory"
    (let [dir (str (System/getProperty "java.io.tmpdir") "/clojask-test-aeron")]
      (try
        (System/setProperty "clojask.zookeeper.port" "2199")
        (System/setProperty "clojask.aeron.port" "40211")
        (System/setProperty "clojask.aeron.dir" dir)
        (compute-still-works)
        (is (= 2199 (:zookeeper.server/port oc/env-config)))
        (is (= "127.0.0.1:2199" (:zookeeper/address oc/peer-config)))
        (is (= 40211 (:onyx.messaging/peer-port oc/peer-config)))
        (is (= dir (:onyx.messaging.aeron/media-driver-dir oc/peer-config)))
        (finally
          (System/clearProperty "clojask.zookeeper.port")
          (System/clearProperty "clojask.aeron.port")
          (System/clearProperty "clojask.aeron.dir"))))
    (compute-still-works)
    (is (= 2188 (:zookeeper.server/port oc/env-config)) "defaults are back")
    (is (nil? (:onyx.messaging.aeron/media-driver-dir oc/peer-config)))))

(deftest log-stays-small
  (testing "a compute adds no :info chatter to the Onyx log"
    (let [log (io/file oc/log-path)
          before (if (.exists log) (.length log) 0)]
      (compute-still-works)
      (is (< (- (.length log) before) 20000)
          "Onyx's default :info logging adds a few hundred KB per compute"))))

(deftest compute-keeps-debug-log-level
  (testing "The log configuration handed to Onyx leaves enable-debug in effect"
    (try
      (ck/enable-debug)
      (compute-still-works)
      (is (= :debug (:min-level timbre/*config*)))
      (finally
        (ck/disable-debug)))))
