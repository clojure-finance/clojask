(ns engine-test
  "The same computes on the local engine and on Onyx. While the local
   engine runs, starting Onyx fails the test. Starts Onyx."
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojask.dataframe :as ck]
            [clojask.onyx-comps :as oc]
            [clojask.api.aggregate :as agg]
            [clojask.api.gb-aggregate :as gb])
  (:import [com.clojask.exception ExecutionException]))

(def input "test/clojask/Employees-example.csv")
(def dir "test/clojask/test_outputs/engine/")

(defn- on-engine
  [engine f]
  (let [old (System/getProperty "clojask.engine")]
    (System/setProperty "clojask.engine" engine)
    (try
      (if (= engine "local")
        (with-redefs [oc/with-onyx-env (fn [& _] (throw (AssertionError. "the local engine started Onyx")))]
          (f))
        (f))
      (finally
        (if old
          (System/setProperty "clojask.engine" old)
          (System/clearProperty "clojask.engine"))))))

(defn- lines [path]
  (with-open [r (io/reader path)] (vec (line-seq r))))

(defn- result
  "The header and the sorted rows that compute writes for the dataframe
   (make-df) on engine, to a file or, with :in-memory, to the group store."
  [engine make-df & opts]
  (on-engine engine
             (fn []
               (let [path (str dir engine ".csv")]
                 (io/make-parents path)
                 (apply ck/compute (make-df) 2 path opts)
                 (let [[header & rows] (lines path)]
                   (into [header] (sort rows)))))))

(defn- same-on-both
  [make-df & opts]
  (let [onyx (apply result "onyx" make-df opts)]
    (is (< 1 (count onyx)) "the computed frame has rows")
    (is (= onyx (apply result "local" make-df opts)))))

(defn- salaries []
  (doto (ck/dataframe input)
    (ck/set-type "Salary" "double")))

(deftest aggregate-only
  (same-on-both #(doto (salaries)
                   (ck/filter "Salary" (fn [s] (<= s 800)))
                   (ck/aggregate agg/max ["Salary"] ["max"])
                   (ck/aggregate agg/min ["Salary"] ["min"])
                   (ck/aggregate agg/count ["Employee"] ["n"]))))

(deftest group-aggregate
  (doseq [in-memory [false true]]
    (testing (str "in-memory " in-memory)
      (same-on-both #(doto (salaries)
                       (ck/group-by ["Department"])
                       (ck/aggregate gb/max ["Salary"] ["max"])
                       (ck/aggregate gb/mean ["Salary"] ["mean"]))
                    :in-memory in-memory))))

(deftest group-aggregate-multi-row-result
  (same-on-both #(doto (salaries)
                   (ck/group-by ["Department"])
                   (ck/aggregate gb/smallest3 ["Salary"] ["low"]))
                :select ["low" "Department"]))

(deftest group-by-only
  (same-on-both #(doto (ck/dataframe input)
                   (ck/group-by ["Department"]))))

(def ^:private boom?
  "Set once the dataframe is built, so the preview that checks each
   aggregate does not throw."
  (atom false))

(defn- boom [f]
  (fn [& args]
    (if @boom?
      (throw (RuntimeException. "boom"))
      (apply f args))))

(deftest aggregate-failure-is-staged
  (doseq [engine ["onyx" "local"]
          [label make-df] [["aggregate" #(ck/aggregate (salaries) (boom agg/max) ["Salary"] ["x"])]
                           ["group aggregate" #(doto (salaries)
                                                 (ck/group-by ["Department"])
                                                 (ck/aggregate (boom gb/max) ["Salary"] ["x"]))]]]
    (testing (str engine " " label)
      (let [df (make-df)]
        (reset! boom? true)
        (try
          (is (thrown-with-msg? ExecutionException #"boom"
                                (on-engine engine #(ck/compute df 2 (str dir "failed.csv")))))
          (finally
            (reset! boom? false))))
      (is (empty? (rest (file-seq (io/file ".clojask/grouped"))))
          "the group files are deleted"))))
