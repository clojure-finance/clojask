(ns core-test
    (:require [clojure.test :refer :all]
              [clojask.dataframe :refer :all]
              [clojask.utils :refer :all]
              [clojask.groupby :refer :all]
              [clojask.api.gb-aggregate :as gb-aggre]
              [clojask.api.aggregate :as aggre]
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
    (is (= clojask.dataframe.DataFrame (type (aggregate y max ["Salary"] ["Salary-max"]))))
    ;(is (= "success" (compute y 8 "resources/test.csv" :exception false)))
    ))

(deftest df-api-output-test
    (testing "Single dataframe manipulation APIs"
    (def y (dataframe "test/clojask/Employees-example.csv" :have-col true))
    ;; element-operation
    (set-type y "Salary" "double")
    (operate y - "Salary")
    (compute y 8 "test/clojask/test_outputs/1-1.csv" :exception false)
    (let [result (sh "zsh" "-c" "diff <(sort ./test/clojask/test_outputs/1-1.csv) <(sort ./test/clojask/correct_outputs/1-1.csv)")]
        (is (= "" (:out result))) 
        (is (= "" (:err result))))
    ;; filter and row-operation
    (def y (dataframe "test/clojask/Employees-example.csv" :have-col true))
    (set-type y "Salary" "double")
    (filter y "Salary" (fn [salary] (<= salary 800)))
    (operate y str ["Employee" "Salary"] "new-col")
    (compute y 8 "test/clojask/test_outputs/1-2.csv" :exception false)
    (let [result (sh "zsh" "-c" "diff <(sort ./test/clojask/test_outputs/1-2.csv) <(sort ./test/clojask/correct_outputs/1-2.csv)")]
        (is (= "" (:out result))) 
        (is (= "" (:err result))))
    ;; groupby and aggregate
    (def y (dataframe "test/clojask/Employees-example.csv" :have-col true))
    (set-type y "Salary" "double")
    (group-by y ["Department"])
    (aggregate y gb-aggre/max ["Salary"] ["new-Salary"])
    (compute y 8 "test/clojask/test_outputs/1-3.csv" :exception false)
    (let [result (sh "zsh" "-c" "diff <(sort test/clojask/test_outputs/1-3.csv) <(sort test/clojask/correct_outputs/1-3.csv)")]
        (is (= "" (:out result))) 
        (is (= "" (:err result))))
    ))

(deftest col-api-test
    (testing "Column manipulation APIs"
    (def y (dataframe "test/clojask/Employees-example.csv" :have-col true))
    (reorder-col y ["Employee" "Department" "EmployeeName" "Salary" "UpdateDate"])
    (is (= (col-names y) ["Employee" "Department" "EmployeeName" "Salary" "UpdateDate"]))
    (rename-col y ["Employee" "new-Department" "EmployeeName" "Salary" "UpdateDate"])
    (is (= (col-names y) ["Employee" "new-Department" "EmployeeName" "Salary" "UpdateDate"]))
    ;; (select-col y ["Employee" "new-Department" "EmployeeName"])
    ;; (is (= (col-names y) ["Employee" "new-Department" "EmployeeName"]))
    ;; (delete-col y ["new-Department"])
    ;; (is (= (col-names y) ["Employee" "EmployeeName"]))
    ))

(deftest col-select-output-test
    (testing "Select column(s) argument"
    (def y (dataframe "test/clojask/Employees-example.csv" :have-col true))
    (compute y 8 "test/clojask/test_outputs/1-9.csv" :select ["Employee", "EmployeeName"] :exception false)
    (let [result (sh "zsh" "-c" "diff <(sort test/clojask/test_outputs/1-9.csv) <(sort test/clojask/correct_outputs/1-9.csv)")]
        (is (= "" (:out result))) 
        (is (= "" (:err result))))
    ))

(deftest join-api-test
    (testing "Join dataframes APIs"
    (def x (dataframe "test/clojask/Employees-example.csv"))
    (def y (dataframe "test/clojask/Employees-example.csv"))
    (is (= clojask.dataframe.DataFrame (type (compute (left-join x y ["Employee"] ["Employee"]) 8 "resources/test.csv" :exception false))))
    (is (= clojask.dataframe.DataFrame (type (compute (right-join x y ["Employee"] ["Employee"]) 8 "resources/test.csv" :exception false))))
    (is (= clojask.dataframe.DataFrame (type (compute (inner-join x y ["Employee"] ["Employee"]) 8 "resources/test.csv" :exception false))))
    (is (= clojask.dataframe.DataFrame (type (compute (rolling-join-forward x y ["Employee"] ["Employee"] "Salary" "Salary") 8 "resources/test.csv" :exception false))))
    ))

(deftest join-api-output-test
    (testing "Join dataframes APIs"
    (def x (dataframe "test/clojask/Employees-example.csv"))
    (def y (dataframe "test/clojask/Employees-info-example.csv"))
    (compute (left-join x y ["Employee"] ["Employee"]) 8 "test/clojask/test_outputs/1-4.csv" :exception false)
    (let [result (sh "zsh" "-c" "diff <(sort test/clojask/test_outputs/1-4.csv) <(sort test/clojask/correct_outputs/1-4.csv)")]
        (is (= "" (:out result))) 
        (is (= "" (:err result))))
    (compute (right-join x y ["Employee"] ["Employee"]) 8 "test/clojask/test_outputs/1-5.csv" :exception false)
    (let [result (sh "zsh" "-c" "diff <(sort test/clojask/test_outputs/1-5.csv) <(sort test/clojask/correct_outputs/1-5.csv)")]
        (is (= "" (:out result))) 
        (is (= "" (:err result))))
    (compute (inner-join x y ["Employee"] ["Employee"]) 8 "test/clojask/test_outputs/1-6.csv" :exception false)
    (let [result (sh "zsh" "-c" "diff <(sort test/clojask/test_outputs/1-6.csv) <(sort test/clojask/correct_outputs/1-6.csv)")]
        (is (= "" (:out result))) 
        (is (= "" (:err result))))
    (compute (rolling-join-forward x y ["EmployeeName"] ["EmployeeName"] "UpdateDate" "UpdateDate") 8 "test/clojask/test_outputs/1-7.csv" :exception false)
    (let [result (sh "zsh" "-c" "diff <(sort test/clojask/test_outputs/1-7.csv) <(sort test/clojask/correct_outputs/1-7.csv)")]
        (is (= "" (:out result))) 
        (is (= "" (:err result))))
    (compute (rolling-join-backward x y ["EmployeeName"] ["EmployeeName"] "UpdateDate" "UpdateDate") 8 "test/clojask/test_outputs/1-8.csv" :exception false)
    (let [result (sh "zsh" "-c" "diff <(sort test/clojask/test_outputs/1-8.csv) <(sort test/clojask/correct_outputs/1-8.csv)")]
        (is (= "" (:out result))) 
        (is (= "" (:err result))))
    ))

