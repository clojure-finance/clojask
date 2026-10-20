(ns engine-test
  "Every kind of compute against the output Onyx wrote for it before
   clojask 2.1 removed Onyx, recorded in correct_outputs/engine.edn: per
   case the header and the sorted rows."
  (:require [clojure.test :refer :all]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojask.dataframe :as ck]
            [clojask.api.aggregate :as agg]
            [clojask.api.gb-aggregate :as gb])
  (:import [com.clojask.exception ExecutionException]))

(def input "test/clojask/Employees-example.csv")
(def info "test/clojask/Employees-info-example.csv")
(def dir "test/clojask/test_outputs/engine/")

(def ^:private expected
  (delay (edn/read-string (slurp "test/clojask/correct_outputs/engine.edn"))))

(defn- lines [path]
  (with-open [r (io/reader path)] (vec (line-seq r))))

(defn- result
  "The header and the sorted rows that compute writes for the dataframe
   (make-df)."
  [make-df & opts]
  (let [path (str dir "result.csv")]
    (io/make-parents path)
    (apply ck/compute (make-df) 2 path opts)
    (let [[header & rows] (lines path)]
      (into [header] (sort rows)))))

(defn- matches
  "Compute (make-df) with opts and compare with the recorded output of
   case k."
  [k make-df & opts]
  (let [want (get @expected k)]
    (is (< 1 (count want)) (str "case " k " is recorded with rows"))
    (is (= want (apply result make-df opts)) k)))

(defn- salaries []
  (doto (ck/dataframe input)
    (ck/set-type "Salary" "double")))

(deftest aggregate-only
  (matches "aggregate-only"
           #(doto (salaries)
              (ck/filter "Salary" (fn [s] (<= s 800)))
              (ck/aggregate agg/max ["Salary"] ["max"])
              (ck/aggregate agg/min ["Salary"] ["min"])
              (ck/aggregate agg/count ["Employee"] ["n"]))))

(deftest group-aggregate
  (doseq [in-memory [false true]]
    (matches (str "group-aggregate, in-memory " in-memory)
             #(doto (salaries)
                (ck/group-by ["Department"])
                (ck/aggregate gb/max ["Salary"] ["max"])
                (ck/aggregate gb/mean ["Salary"] ["mean"]))
             :in-memory in-memory)))

(deftest group-aggregate-multi-row-result
  (matches "group-aggregate-multi-row-result"
           #(doto (salaries)
              (ck/group-by ["Department"])
              (ck/aggregate gb/smallest3 ["Salary"] ["low"]))
           :select ["low" "Department"]))

(deftest group-by-only
  (matches "group-by-only"
           #(doto (ck/dataframe input)
              (ck/group-by ["Department"]))))

(def ^:private boom?
  "Set once the dataframe is built, so the preview that checks each
   function does not throw."
  (atom false))

(defn- boom [f]
  (fn [& args]
    (if @boom?
      (throw (RuntimeException. "boom"))
      (apply f args))))

(defn- fails-staged
  "Computing df throws an ExecutionException about boom, and the group
   files are gone afterwards."
  [df & opts]
  (reset! boom? true)
  (try
    (is (thrown-with-msg? ExecutionException #"boom"
                          (apply ck/compute df 2 (str dir "failed.csv") opts)))
    (finally
      (reset! boom? false)))
  (doseq [d [".clojask/grouped" ".clojask/join"]]
    (is (empty? (remove #(.isDirectory %) (file-seq (io/file d))))
        (str "the files under " d " are deleted"))))

(deftest aggregate-failure-is-staged
  (testing "aggregate"
    (fails-staged (ck/aggregate (salaries) (boom agg/max) ["Salary"] ["x"])))
  (testing "group aggregate"
    (fails-staged (doto (salaries)
                    (ck/group-by ["Department"])
                    (ck/aggregate (boom gb/max) ["Salary"] ["x"])))))

(defn- employees
  "The employees with a filter, an operation and a formatter, so the join
   workers have work to do."
  []
  (doto (salaries)
    (ck/filter "Salary" (fn [s] (<= s 1000)))
    (ck/operate - "Salary")
    (ck/set-formatter "Salary" #(str % "!"))))

(defn- infos []
  (doto (ck/dataframe info)
    (ck/set-formatter "DayOff" #(str "d" %))))

(def ^:private joins
  "Label and join of each kind. employees is the larger side, so inner and
   outer joins swap when it comes second."
  [["inner" #(ck/inner-join (employees) (infos) ["Employee"] ["Employee"])]
   ["inner swapped" #(ck/inner-join (infos) (employees) ["Employee"] ["Employee"])]
   ["left" #(ck/left-join (employees) (infos) ["Employee"] ["Employee"])]
   ["right" #(ck/right-join (employees) (infos) ["Employee"] ["Employee"])]
   ["outer" #(ck/outer-join (employees) (infos) ["Employee"] ["Employee"])]
   ["outer swapped" #(ck/outer-join (infos) (employees) ["Employee"] ["Employee"])]
   ["rolling forward" #(ck/rolling-join-forward (employees) (infos) ["EmployeeName"] ["EmployeeName"] "UpdateDate" "UpdateDate")]
   ["rolling backward" #(ck/rolling-join-backward (employees) (infos) ["EmployeeName"] ["EmployeeName"] "UpdateDate" "UpdateDate")]
   ["rolling backward with limit" #(ck/rolling-join-backward (employees) (infos) ["Employee"] ["Employee"] "UpdateDate" "UpdateDate"
                                                             :limit (fn [_ b-val] (not= b-val "2020/12/05")))]])

(deftest join
  (doseq [[label make-df] joins
          in-memory [false true]]
    (matches (str "join " label ", in-memory " in-memory)
             make-df :in-memory in-memory)))

(deftest join-select
  (doseq [[label make-df] joins
          in-memory [false true]]
    (matches (str "join-select " label ", in-memory " in-memory)
             make-df :select ["2_UpdateDate" "1_UpdateDate" "1_Employee"] :in-memory in-memory)))

(deftest join-failure-is-staged
  (doseq [[label make-join] [["inner" ck/inner-join] ["outer" ck/outer-join]]
          in-memory [false true]]
    (testing (str label ", in-memory " in-memory)
      (fails-staged (make-join (doto (salaries) (ck/operate (boom -) "Salary")) (infos) ["Employee"] ["Employee"])
                    :exception true :in-memory in-memory))))
