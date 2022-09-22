(ns core-test
    (:require [clojure.test :refer :all]
              [clojask.dataframe :refer :all]
              [clojask.utils :refer :all]
              [clojask.groupby :refer :all]
              [clojask.api.gb-aggregate :as gb-aggre]
              [clojask.api.aggregate :as aggre]
              [clojask.sort :refer :all]))
        
(use '[clojure.java.shell :only [sh]])

(enable-debug)

(deftest df-api-test
  (testing "Single dataframe manipulation APIs"
    (def y (dataframe "test/clojask/Employees-example.csv" :have-col true))
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
    (def y (dataframe "test/clojask/Employees-example.csv" :have-col true))
    ;; element-operation
    (set-type y "Salary" "double")
    (operate y - "Salary")
    (set-formatter y "Salary" #(str % "!"))
    (compute y 8 "test/clojask/test_outputs/1-1.csv" :exception false :order true)
    (let [result (sh "zsh" "-c" "diff <(cat ./test/clojask/test_outputs/1-1.csv) <(cat ./test/clojask/correct_outputs/1-1.csv)")]
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
    ;; aggregate only
    (def y (dataframe "test/clojask/Employees-example.csv" :have-col true))
    (set-type y "Salary" "double")
    (aggregate y aggre/max ["Salary"] ["new-Salary"])
    (compute y 8 "test/clojask/test_outputs/1-10.csv" :exception false)
    (let [result (sh "zsh" "-c" "diff <(sort test/clojask/test_outputs/1-10.csv) <(sort test/clojask/correct_outputs/1-10.csv)")]
        (is (= "" (:out result))) 
        (is (= "" (:err result))))
    ;; groupby only
    (def y (dataframe "test/clojask/Employees-example.csv" :have-col true))
    (group-by y ["Department"])
    (compute y 8 "test/clojask/test_outputs/1-11.csv" :exception false)
    (let [result (sh "zsh" "-c" "diff <(sort test/clojask/test_outputs/1-11.csv) <(sort test/clojask/correct_outputs/1-11.csv)")]
        (is (= "" (:out result))) 
        (is (= "" (:err result))))
    ))

(deftest col-api-test
    (testing "Column manipulation APIs"
      (def y (dataframe "test/clojask/Employees-example.csv" :have-col true))
      (reorder-col y ["Employee" "Department" "EmployeeName" "Salary" "UpdateDate"])
      (is (= (get-col-names y) ["Employee" "Department" "EmployeeName" "Salary" "UpdateDate"]))
      (rename-col y "Department" "new-Department")
    ;; (map (fn [a b] (rename-col y a b)) (get-col-names y) ["Employee" "new-Department" "EmployeeName" "Salary" "UpdateDate"])
      (is (= (get-col-names y) ["Employee" "new-Department" "EmployeeName" "Salary" "UpdateDate"]))))

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
    (is (= clojask.classes.DataFrame.DataFrame (type (compute (left-join x y ["Employee"] ["Employee"]) 8 "resources/test.csv" :exception false))))
    (is (= clojask.classes.DataFrame.DataFrame (type (compute (right-join x y ["Employee"] ["Employee"]) 8 "resources/test.csv" :exception false))))
    (is (= clojask.classes.DataFrame.DataFrame (type (compute (inner-join x y ["Employee"] ["Employee"]) 8 "resources/test.csv" :exception false))))
    (is (= clojask.classes.DataFrame.DataFrame (type (compute (rolling-join-forward x y ["Employee"] ["Employee"] "Salary" "Salary") 8 "resources/test.csv" :exception false))))
    ))

(deftest join-api-output-test
    (testing "Join dataframes APIs"
      (def x (dataframe "test/clojask/Employees-example.csv"))
      (set-type x "UpdateDate" "date:yyyy/MM/dd")
      (def y (dataframe "test/clojask/Employees-info-example.csv"))
      (set-type y "UpdateDate" "date:yyyy/MM/dd")
      (compute (left-join x y ["Employee"] ["Employee"]) 8 "test/clojask/test_outputs/1-4.csv" :exception false)
      (let [result (sh "zsh" "-c" "diff <(sort test/clojask/test_outputs/1-4.csv) <(sort test/clojask/correct_outputs/1-4.csv)")]
        (is (= "" (:out result)))
        (is (= "" (:err result))))
      (compute (right-join x y ["Employee"] ["Employee"]) 8 "test/clojask/test_outputs/1-5.csv" :exception false)
      (let [result (sh "zsh" "-c" "diff <(sort test/clojask/test_outputs/1-5.csv) <(sort test/clojask/correct_outputs/1-5.csv)")]
        (is (= "" (:out result)))
        (is (= "" (:err result))))
      (def z (inner-join x y ["Employee"] ["Employee"]))
      (compute z 8 "test/clojask/test_outputs/1-6.csv" :exception false :select ["2_Employee" "2_EmployeeName" "2_DayOff" "2_UpdateDate" "1_Employee" "1_EmployeeName" "1_Department" "1_Salary" "1_UpdateDate"])
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
      (compute (outer-join x y ["Employee"] ["Employee"]) 8 "test/clojask/test_outputs/1-12.csv" :select ["1_Department" "1_Salary" "1_UpdateDate" "2_Employee" "2_EmployeeName" "2_DayOff" "2_UpdateDate"])
      (let [result (sh "zsh" "-c" "diff <(sort test/clojask/test_outputs/1-12.csv) <(sort test/clojask/correct_outputs/1-12.csv)")]
        (is (= "" (:out result)))
        (is (= "" (:err result))))))

