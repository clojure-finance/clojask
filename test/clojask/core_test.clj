(ns core-test
    (:require [clojure.test :refer :all]
              [clojask.dataframe :refer :all]
              [clojask.utils :refer :all]
              [clojask.groupby :refer :all]
              [clojask.sort :refer :all]))
        
(use '[clojure.java.shell :only [sh]])

(deftest df-api-test
  (testing "Single dataframe manipulation APIs"
    (def y (dataframe "test/clojask/Employees-example.csv" :have-col true))
    (is (= clojask.dataframe.DataFrame (type y)))
    (is (= clojask.dataframe.DataFrame (type (set-type y "Salary" "double"))))
    (is (= clojask.dataframe.DataFrame (type (set-parser y "Department" #(Double/parseDouble %)))))
    (is (= clojask.dataframe.DataFrame (type (filter y "Salary" (fn [salary] (<= salary 800))))))
    (is (= clojask.dataframe.DataFrame (type (operate y - "Salary"))))
    (is (= clojask.dataframe.DataFrame (type (operate y str ["Employee" "Salary"] "new-col"))))
    (is (= clojask.dataframe.DataFrame (type (group-by y ["Department"]))))
    (is (= clojask.dataframe.DataFrame (type (aggregate y min ["Employee"] ["new-employee"]))))
    ;(is (= "success" (compute y 8 "resources/test.csv" :exception false)))
    ))

(deftest df-api-output-test
    (testing "Single dataframe manipulation APIs"
    (def y (dataframe "test/clojask/Employees-example.csv" :have-col true))
    ;; element-operation
    (set-type y "Salary" "double")
    (operate y - "Salary")
    (compute y 8 "test/clojask/test_outputs/1-1.csv" :exception false)
    (let [result (sh "diff" "<(sort test/clojask/test_outputs/1-1.csv)" "<(sort test/clojask/correct_outputs/1-1.csv)")]
        (is (= "" (:out result))))
    ;; filter and row-operation
    (def y (dataframe "test/clojask/Employees-example.csv" :have-col true))
    (set-type y "Salary" "double")
    (filter y "Salary" (fn [salary] (<= salary 800)))
    (operate y str ["Employee" "Salary"] "new-col")
    (compute y 8 "test/clojask/test_outputs/1-2.csv" :exception false)
    (let [result (sh "diff" "<(sort test/clojask/test_outputs/1-2.csv)" "<(sort test/clojask/correct_outputs/1-2.csv)")]
        (is (= "" (:out result))))
    ;; groupby and aggregate
    (def y (dataframe "test/clojask/Employees-example.csv" :have-col true))
    (group-by y ["Department"])
    (aggregate y min ["Employee"] ["new-employee"])
    (compute y 8 "test/clojask/test_outputs/1-3.csv" :exception false)
    (let [result (sh "diff" "<(sort test/clojask/test_outputs/1-3.csv)" "<(sort test/clojask/correct_outputs/1-3.csv)")]
        (is (= "" (:out result))))
    ))

(deftest col-api-test
    (testing "Column manipulation APIs"
    (def y (dataframe "test/clojask/Employees-example.csv" :have-col true))
    (reorder-col y ["Employee" "Department" "EmployeeName" "Salary"])
    (is (= (col-names y) ["Employee" "Department" "EmployeeName" "Salary"]))
    (rename-col y ["Employee" "new-Department" "EmployeeName" "Salary"])
    (is (= (col-names y) ["Employee" "new-Department" "EmployeeName" "Salary"]))
    (delete-col y ["new-Department" "Salary"])
    (is (= (col-names y) ["Employee" "EmployeeName"]))
    ))

(deftest join-api-test
    (testing "Join dataframes APIs"
    (def x (dataframe "test/clojask/Employees-example.csv"))
    (def y (dataframe "test/clojask/Employees-example.csv"))
    (is (= "success" (left-join x y ["Employee"] ["Employee"] 8 "resources/test.csv" :exception false)))
    (is (= "success" (right-join x y ["Employee"] ["Employee"] 8 "resources/test.csv" :exception false)))
    (is (= "success" (inner-join x y ["Employee"] ["Employee"] 8 "resources/test.csv" :exception false)))
    (is (= "success" (rolling-join-forward x y ["Employee"] ["Employee"] "Salary" "Salary" 8 "resources/test.csv" :exception false)))
    ))

(deftest join-api-output-test
    (testing "Join dataframes APIs"
    (def x (dataframe "test/clojask/Employees-example.csv"))
    (def y (dataframe "test/clojask/Employees-info-example.csv"))
    (left-join x y ["Employee"] ["Employee"] 8 "test/clojask/test_outputs/1-4.csv" :exception false)
    (let [result (sh "diff" "<(sort test/clojask/test_outputs/1-4.csv)" "<(sort test/clojask/correct_outputs/1-4.csv)")]
        (is (= "" (:out result))))
    (right-join x y ["Employee"] ["Employee"] 8 "test/clojask/test_outputs/1-5.csv" :exception false)
    (let [result (sh "diff" "<(sort test/clojask/test_outputs/1-5.csv)" "<(sort test/clojask/correct_outputs/1-5.csv)")]
        (is (= "" (:out result))))
    (inner-join x y ["Employee"] ["Employee"] 8 "test/clojask/test_outputs/1-6.csv" :exception false)
    (let [result (sh "diff" "<(sort test/clojask/test_outputs/1-6.csv)" "<(sort test/clojask/correct_outputs/1-6.csv)")]
        (is (= "" (:out result))))
    (rolling-join-forward x y ["EmployeeName"] ["EmployeeName"] "UpdateDate" "UpdateDate" 8 "test/clojask/test_outputs/1-7.csv" :exception false)
    (let [result (sh "diff" "<(sort test/clojask/test_outputs/1-7.csv)" "<(sort test/clojask/correct_outputs/1-7.csv)")]
        (is (= "" (:out result))))
    (rolling-join-backward x y ["EmployeeName"] ["EmployeeName"] "UpdateDate" "UpdateDate" 8 "test/clojask/test_outputs/1-8.csv" :exception false)
    (let [result (sh "diff" "<(sort test/clojask/test_outputs/1-8.csv)" "<(sort test/clojask/correct_outputs/1-8.csv)")]
        (is (= "" (:out result))))
    ))

