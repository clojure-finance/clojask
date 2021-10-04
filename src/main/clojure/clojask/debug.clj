(ns clojask.debug
  (:require [clojask.DataFrame :refer :all]
            [clojask.utils :refer :all]
            [clojask.groupby :refer :all]
            [clojask.sort :refer :all]))
"For debugging purposes only, will not be used in production."

(import '[com.stackoverflow.clojure MyOwnException])

(defn -main
  []
  (def x "Hello world")
  (-> (clojure.core/format "Expression '%s' not defined." x)(MyOwnException.)(throw))

  ; (def y (dataframe "resources/Employees-large.csv" :have-col true))
  ;; (set-type y "Salary" "double")
  ;; (filter y "Salary" (fn [salary] (<= salary 800)))

  ;; (group-by y ["Department"])
  ;; (aggregate y min ["Employee"] ["new-employee"])

  ;; (set-type y "Department" "double")
  ;; (set-parser y "Salary" #(Double/parseDouble %))
  ;; (operate y - "Department")
  ;; (operate y str ["Employee" "Salary"] "new-col")

  ;; (time (compute y 4 "resources/test.csv" :exception true :order true))

  ;; (-> (dataframe "resources/Employees-large.csv" :have-col true)
  ;;     (set-type "Salary" "double")
  ;;     (filter "Salary" (fn [salary] (<= salary 800)))
  ;;     (set-type "Department" "double")
  ;;     (operate - "Department")
  ;;     (operate str ["Employee" "Salary"] "new-col")
  ;;     (group-by ["Department"])
  ;;     (aggregate min ["Employee"] ["new-employee"])
  ;;     (compute 4 "resources/test.csv" :exception true :order true)
  ;;     time)
  
  ;; (println (.getKeys (.col-info y)))
  ;; ;(println "Renaming columns...")
  ;; (filter y ["Salary" "Department"] (fn [salary] (<= salary 800)))
  ;; (filter y ["Salary" "Department"] (fn [salary] (<= salary 800)))
  ;; (group-by y "Department")
  ;; (aggregate y aggre-avg ["Department" "Salary"] ["dept-avg" "salary-avg"])
  ;; (reorder-col y ["Employee" "Department-x" "EmployeeName" "Salary"])
  ;; (.reorderCol (.row-info y) (.getDesc (.col-info y)) ["Employee" "Department" "EmployeeName" "Salary"])
  ;; (println (.getKeys (.col-info y)))
  
  ;; Benchmarking

  ;(def y (dataframe "../clojure-datasets/data-CRSP.csv"))
  ;(def y (dataframe "resources/data-CRSP.csv" :have-col true))
  ;(set-type y "prccq" "double")
  ;(operate y - "prccq")
  ;(operate y str ["PERMCO" "PERMNO"] "new-col")
  ;(group-by y "gvkey")
  ;(aggregate y min ["prccq"] ["prccq-min"])
  ;(group-by y "PERMCO")
  ;(aggregate y min ["PERMNO"] ["PERMNO-min"])
  ;(time (compute y 4 "resources/test.csv" :exception false))

  ;; (def x (dataframe "../clojure-datasets/CRSP-extract.csv"))
  ;; (def y (dataframe "../clojure-datasets/data-Compustat-lohi.csv"))
  ;; (time (left-join x y ["date"] ["datadate"] 4 "resources/test.csv" :exception false))
  )