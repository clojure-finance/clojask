(ns core-test
    (:require [clojure.test :refer :all]
              [clojask.DataFrame :refer :all]
              [clojask.utils :refer :all]
              [clojask.groupby :refer :all]
              [clojask.sort :refer :all]))
        
(use '[clojure.java.shell :only [sh]])

(deftest df-api-test
  (testing "Single dataframe manipulation APIs"
    (def y (dataframe "test/clojask/Employees-example.csv" :have-col true))
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

(deftest df-api-output-test
    (testing "Single dataframe manipulation APIs"
    (def y (dataframe "test/clojask/Employees-example.csv" :have-col true))
    ;; element-operation
    (set-type y "Salary" "double")
    (operate y - "Salary")
    (compute y 8 "test/clojask/test_outputs/1-1.csv" :exception true :order true)
    (let [result (sh "diff" "<(sort test/clojask/test_outputs/1-1.csv)" "<(sort test/clojask/correct_outputs/1-1.csv)")]
        (is (= "" (:out result))))
    ;; filter and row-operation
    (def y (dataframe "test/clojask/Employees-example.csv" :have-col true))
    (set-type y "Salary" "double")
    (filter y "Salary" (fn [salary] (<= salary 800)))
    (operate y str ["Employee" "Salary"] "new-col")
    (compute y 8 "test/clojask/test_outputs/1-2.csv" :exception true :order true)
    (let [result (sh "diff" "<(sort test/clojask/test_outputs/1-2.csv)" "<(sort test/clojask/correct_outputs/1-2.csv)")]
        (is (= "" (:out result))))
    ;; groupby and aggregate
    (def y (dataframe "test/clojask/Employees-example.csv" :have-col true))
    (group-by y ["Department"])
    (aggregate y min ["Employee"] ["new-employee"])
    (compute y 8 "test/clojask/test_outputs/1-3.csv" :exception true :order true)
    (let [result (sh "diff" "<(sort test/clojask/test_outputs/1-3.csv)" "<(sort test/clojask/correct_outputs/1-3.csv)")]
        (is (= "" (:out result))))
    ))

(deftest col-api-test
    (testing "Column manipulation APIs"
    (def y (dataframe "test/clojask/Employees-example.csv" :have-col true))
    (reorder-col y ["Employee" "Department" "EmployeeName" "Salary"])
    (is (= (.getKeys (.col-info y)) ["Employee" "Department" "EmployeeName" "Salary"]))
    (rename-col y ["Employee" "new-Department" "EmployeeName" "Salary"])
    (is (= (.getKeys (.col-info y)) ["Employee" "new-Department" "EmployeeName" "Salary"]))
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
    (def y (dataframe "test/clojask/Employees-example.csv"))
    (left-join x y ["Employee"] ["Employee"] 8 "test/clojask/test_outputs/1-4.csv" :exception false)
    (let [result (sh "diff" "<(sort test/clojask/test_outputs/1-4.csv)" "<(sort test/clojask/correct_outputs/1-4.csv)")]
        (is (= "" (:out result))))
    (right-join x y ["Employee"] ["Employee"] 8 "test/clojask/test_outputs/1-5.csv" :exception false)
    (let [result (sh "diff" "<(sort test/clojask/test_outputs/1-5.csv)" "<(sort test/clojask/correct_outputs/1-5.csv)")]
        (is (= "" (:out result))))
    (inner-join x y ["Employee"] ["Employee"] 8 "test/clojask/test_outputs/1-6.csv" :exception false)
    (let [result (sh "diff" "<(sort test/clojask/test_outputs/1-6.csv)" "<(sort test/clojask/correct_outputs/1-6.csv)")]
        (is (= "" (:out result))))
    (rolling-join-forward x y ["Employee"] ["Employee"] "Salary" "Salary" 8 "test/clojask/test_outputs/1-7.csv" :exception false)
    (let [result (sh "diff" "<(sort test/clojask/test_outputs/1-7.csv)" "<(sort test/clojask/correct_outputs/1-7.csv)")]
        (is (= "" (:out result))))
    ))

