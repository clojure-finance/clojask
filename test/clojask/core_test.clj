(ns core-test
    (:require [clojure.test :refer :all]
              [clojask.dataframe :refer :all]
              [clojask.utils :refer :all]
              [clojask.groupby :refer :all]
              [clojask.api.gb-aggregate :as gb-aggre]
              [clojask.api.aggregate :as aggre]
              [clojask.sort :refer :all]
              [clojure.string :as str]
              [clojure.java.io :as io])
    (:refer-clojure :exclude [filter group-by sort]))

;; The output directory is gitignored, so create it before any test writes to it.
(io/make-parents "test/clojask/test_outputs/.keep")

(defn get-diff
  "Compare an output file with the expected one. The header line must match
   exactly; data rows are compared sorted (Onyx writes them in any order)
   unless order is false."
  [a b & [order]]
  (try
    (let [order (if (nil? order) true order)
          [head-a & rows-a] (str/split-lines (slurp a))
          [head-b & rows-b] (str/split-lines (slurp b))
          rows-a (if order (clojure.core/sort rows-a) rows-a)
          rows-b (if order (clojure.core/sort rows-b) rows-b)]
      (if (and (= head-a head-b) (= (vec rows-a) (vec rows-b)))
        {:out "" :err ""}
        {:out (str "not the same: " (cons head-a rows-a) (cons head-b rows-b)) :err ""}))
    (catch Exception e {:out "has exception" :err (str e)})))

;; (enable-debug)

(deftest df-api-test
  (testing "Single dataframe manipulation APIs"
    (def y (dataframe "test/clojask/Employees-example.csv" :if-header true))
    (is (= clojask.classes.DataFrame.DataFrame (type y)))
    (is (= clojask.classes.DataFrame.DataFrame (type (set-type y "Salary" "double"))))
    (is (= clojask.classes.DataFrame.DataFrame (type (set-parser y "Department" #(Double/parseDouble %)))))
    (is (= clojask.classes.DataFrame.DataFrame (type (filter y "Salary" (fn [salary] (<= salary 800))))))
    (is (= clojask.classes.DataFrame.DataFrame (type (operate y - "Salary"))))
    (is (= clojask.classes.DataFrame.DataFrame (type (operate y str ["Employee" "Salary"] "new-col"))))
    (is (= clojask.classes.DataFrame.DataFrame (type (group-by y ["Department"]))))
    (is (= clojask.classes.DataFrame.DataFrame (type (aggregate y max ["Salary"] ["Salary-max"]))))
    (is (= clojask.classes.DataFrame.DataFrame (type (compute y 8 "test/clojask/test_outputs/tmp.csv"))))
    ))

(deftest df-api-output-test
    (testing "Single dataframe manipulation APIs"
    (def y (dataframe "test/clojask/Employees-example.csv" :if-header true))
    ;; element-operation
    (set-type y "Salary" "double")
    (operate y - "Salary")
    (set-formatter y "Salary" #(str % "!"))
    (compute y 8 "test/clojask/test_outputs/1-1.csv" :exception false :order true)
    ;; filter and row-operation
    (def y (dataframe "test/clojask/Employees-example.csv" :if-header true))
    (set-type y "Salary" "double")
    (filter y "Salary" (fn [salary] (<= salary 800)))
    (operate y str ["Employee" "Salary"] "new-col")
    (compute y 8 "test/clojask/test_outputs/1-2.csv" :exception false)
    ;; groupby and aggregate
    (def y (dataframe "test/clojask/Employees-example.csv" :if-header true))
    (set-type y "Salary" "double")
    (group-by y ["Department"])
    (aggregate y gb-aggre/max ["Salary"] ["new-Salary"])
    (compute y 8 "test/clojask/test_outputs/1-3.csv" :exception false)
    ;; aggregate only
    (def y (dataframe "test/clojask/Employees-example.csv" :if-header true))
    (set-type y "Salary" "double")
    (aggregate y aggre/max ["Salary"] ["new-Salary"])
    (compute y 8 "test/clojask/test_outputs/1-10.csv" :exception false)
    ;; groupby only
    (def y (dataframe "test/clojask/Employees-example.csv" :if-header true))
    (group-by y ["Department"])
    (compute y 8 "test/clojask/test_outputs/1-11.csv" :exception false)
    ))

(deftest col-api-test
    (testing "Column manipulation APIs"
      (def y (dataframe "test/clojask/Employees-example.csv" :if-header true))
      (reorder-col y ["Employee" "Department" "EmployeeName" "Salary" "UpdateDate"])
      (is (= (get-col-names y) ["Employee" "Department" "EmployeeName" "Salary" "UpdateDate"]))
      (rename-col y "Department" "new-Department")
    ;; (map (fn [a b] (rename-col y a b)) (get-col-names y) ["Employee" "new-Department" "EmployeeName" "Salary" "UpdateDate"])
      (is (= (get-col-names y) ["Employee" "new-Department" "EmployeeName" "Salary" "UpdateDate"]))))

(deftest col-select-output-test
    (testing "Select column(s) argument"
    (def y (dataframe "test/clojask/Employees-example.csv" :if-header true))
    (compute y 8 "test/clojask/test_outputs/1-9.csv" :select ["Employee", "EmployeeName"] :exception false)
    ))

(deftest join-api-test
    (testing "Join dataframes APIs"
    (def x (dataframe "test/clojask/Employees-example.csv"))
    (def y (dataframe "test/clojask/Employees-example.csv"))
    (is (= clojask.classes.DataFrame.DataFrame (type (compute (left-join x y ["Employee"] ["Employee"]) 8 "test/clojask/test_outputs/join-api.csv" :exception false))))
    (is (= clojask.classes.DataFrame.DataFrame (type (compute (right-join x y ["Employee"] ["Employee"]) 8 "test/clojask/test_outputs/join-api.csv" :exception false))))
    (is (= clojask.classes.DataFrame.DataFrame (type (compute (inner-join x y ["Employee"] ["Employee"]) 8 "test/clojask/test_outputs/join-api.csv" :exception false))))
    (is (= clojask.classes.DataFrame.DataFrame (type (compute (rolling-join-forward x y ["Employee"] ["Employee"] "Salary" "Salary") 8 "test/clojask/test_outputs/join-api.csv" :exception false))))
    ))

(deftest join-api-output-test
    (testing "Join dataframes APIs"
      (def x (dataframe "test/clojask/Employees-example.csv"))
      (set-type x "UpdateDate" "date:yyyy/MM/dd")
      (def y (dataframe "test/clojask/Employees-info-example.csv"))
      (set-type y "UpdateDate" "date:yyyy/MM/dd")
      (compute (left-join x y ["Employee"] ["Employee"]) 8 "test/clojask/test_outputs/1-4.csv" :exception false)
      (compute (right-join x y ["Employee"] ["Employee"]) 8 "test/clojask/test_outputs/1-5.csv" :exception false)
      (def z (inner-join x y ["Employee"] ["Employee"]))
      (compute z 8 "test/clojask/test_outputs/1-6.csv" :exception false :select ["2_Employee" "2_EmployeeName" "2_DayOff" "2_UpdateDate" "1_Employee" "1_EmployeeName" "1_Department" "1_Salary" "1_UpdateDate"])
      (compute (rolling-join-forward x y ["EmployeeName"] ["EmployeeName"] "UpdateDate" "UpdateDate") 8 "test/clojask/test_outputs/1-7.csv" :exception false)
      (compute (rolling-join-backward x y ["EmployeeName"] ["EmployeeName"] "UpdateDate" "UpdateDate") 8 "test/clojask/test_outputs/1-8.csv" :exception false)
      (compute (outer-join x y ["Employee"] ["Employee"]) 8 "test/clojask/test_outputs/1-12.csv" :select ["1_Department" "1_Salary" "1_UpdateDate" "2_Employee" "2_EmployeeName" "2_DayOff" "2_UpdateDate"])
      ))

(deftest test-ns-hook
  (testing "Check all the outputs in a nested way"
    (df-api-test)
    (df-api-output-test)
    (col-api-test)
    (col-select-output-test)
    (join-api-test)
    (join-api-output-test)
    (let [result (get-diff "./test/clojask/test_outputs/1-1.csv" "./test/clojask/correct_outputs/1-1.csv" false)]
      (is (= "" (:out result)))
      (is (= "" (:err result))))
    (let [result (get-diff "./test/clojask/test_outputs/1-2.csv" "./test/clojask/correct_outputs/1-2.csv")]
      (is (= "" (:out result)))
      (is (= "" (:err result))))
    (let [result (get-diff "test/clojask/test_outputs/1-3.csv" "test/clojask/correct_outputs/1-3.csv")]
      (is (= "" (:out result)))
      (is (= "" (:err result))))
    (let [result (get-diff "test/clojask/test_outputs/1-10.csv" "test/clojask/correct_outputs/1-10.csv")]
      (is (= "" (:out result)))
      (is (= "" (:err result))))
    (let [result (get-diff "test/clojask/test_outputs/1-11.csv" "test/clojask/correct_outputs/1-11.csv")]
      (is (= "" (:out result)))
      (is (= "" (:err result))))
    (let [result (get-diff "test/clojask/test_outputs/1-9.csv" "test/clojask/correct_outputs/1-9.csv")]
      (is (= "" (:out result)))
      (is (= "" (:err result))))
    (let [result (get-diff "test/clojask/test_outputs/1-4.csv" "test/clojask/correct_outputs/1-4.csv")]
      (is (= "" (:out result)))
      (is (= "" (:err result))))
    (let [result (get-diff "test/clojask/test_outputs/1-5.csv" "test/clojask/correct_outputs/1-5.csv")]
      (is (= "" (:out result)))
      (is (= "" (:err result))))
    (let [result (get-diff "test/clojask/test_outputs/1-6.csv" "test/clojask/correct_outputs/1-6.csv")]
      (is (= "" (:out result)))
      (is (= "" (:err result))))
    (let [result (get-diff "test/clojask/test_outputs/1-7.csv" "test/clojask/correct_outputs/1-7.csv")]
      (is (= "" (:out result)))
      (is (= "" (:err result))))
    (let [result (get-diff "test/clojask/test_outputs/1-8.csv" "test/clojask/correct_outputs/1-8.csv")]
      (is (= "" (:out result)))
      (is (= "" (:err result))))
    (let [result (get-diff "test/clojask/test_outputs/1-12.csv" "test/clojask/correct_outputs/1-12.csv")]
      (is (= "" (:out result)))
      (is (= "" (:err result))))))

