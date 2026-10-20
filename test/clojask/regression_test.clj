(ns regression-test
  "Computes that each exercised a bug fixed in 2.0.5 or later."
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojask.dataframe :as ck]
            [clojask.api.aggregate :as agg]
            [clojask.api.gb-aggregate :as gb]
            [clojask-io.input :refer [read-excel]])
  (:import [com.clojask.exception TypeException OperationException]))

(def input "test/clojask/Employees-example.csv")
(def dir "test/clojask/test_outputs/regression/")

(defn- out [name] (let [f (str dir name)] (io/make-parents f) f))

(defn- lines [path]
  (with-open [r (io/reader path)] (vec (line-seq r))))

(deftest compute-without-output-path-returns-rows
  (let [df (ck/dataframe input)
        rows (ck/compute df 1 nil)]
    (is (= 8 (count rows)) "header and seven rows")
    (is (= (ck/get-col-names df) (vec (first rows))))))

(deftest select-keeps-order-beyond-eight-columns
  (testing "a dataframe from a function, written to a file, with 10 columns selected in reverse"
    (let [names (mapv #(str "c" %) (range 10))
          df (ck/dataframe (fn [] [names (mapv str (range 10))]))
          path (out "select-order.csv")]
      (ck/compute df 1 path :select (vec (reverse names)))
      (is (= [(str/join "," (reverse names)) (str/join "," (map str (reverse (range 10))))]
             (lines path))))))

(deftest header-false-result-keeps-first-row
  (let [result (ck/compute (ck/dataframe input) 1 (out "no-header.csv") :header false)]
    (is (= 7 (count (lines (out "no-header.csv")))))
    (is (= 7 (count (ck/preview result 100 100))) "the returned dataframe reads no header")))

(deftest rolling-join-backward-picks-nearest-and-applies-limit
  (let [a-path (out "roll-a.csv")
        b-path (out "roll-b.csv")]
    (spit a-path "k,t\nx,2020/12/01\n")
    (spit b-path "k,t,v\nx,2020/12/20,far\nx,2020/12/05,near\n")
    (doseq [in-memory [false true]]
      (testing (str "in-memory " in-memory)
        (let [a (ck/dataframe a-path)
              b (ck/dataframe b-path)
              nearest (out "roll-nearest.csv")
              limited (out "roll-limited.csv")]
          (ck/compute (ck/rolling-join-backward a b ["k"] ["k"] "t" "t") 1 nearest :in-memory in-memory)
          (is (= "x,2020/12/01,x,2020/12/05,near" (second (lines nearest))))
          (ck/compute (ck/rolling-join-backward a b ["k"] ["k"] "t" "t" :limit (fn [_ b-val] (not= b-val "2020/12/05")))
                      1 limited :in-memory in-memory)
          (is (= "x,2020/12/01,x,2020/12/20,far" (second (lines limited)))))))))

(deftest aggregate-over-no-rows-writes-empty-cell
  (let [df (ck/dataframe input)
        path (out "empty-aggregate.csv")]
    (ck/set-type df "Salary" "double")
    (ck/filter df "Salary" (fn [_] false))
    (ck/aggregate df agg/max ["Salary"] ["Salary-max"])
    (ck/compute df 1 path)
    (is (not-any? #(str/includes? % "Unbound") (lines path)))))

(deftest formatter-on-unselected-column-is-not-applied-elsewhere
  (let [df (ck/dataframe input)
        path (out "formatter-position.csv")]
    (ck/set-type df "Salary" "double")
    (ck/set-formatter df "EmployeeName" #(str % "!"))
    (ck/group-by df ["Department"])
    (ck/aggregate df gb/max ["Salary"] ["Salary-max"])
    (ck/compute df 1 path)
    (is (= 5 (count (lines path))) "header and four departments")
    (is (not-any? #(str/includes? % "!") (lines path)))))

(defn- stray-tmp-files [dir]
  (->> (file-seq (io/file dir))
       (remove #(.isDirectory %))
       (mapv str)))

(deftest group-files-deleted-when-compute-finishes
  (let [df (ck/dataframe input)
        path (out "group-clean.csv")]
    (ck/set-type df "Salary" "double")
    (ck/group-by df ["Department"])
    (ck/aggregate df gb/mean ["Salary"] ["mean-salary"])
    (ck/compute df 2 path)
    (is (< 1 (count (lines path))))
    (is (= [] (stray-tmp-files ".clojask/grouped")))))

(deftest join-files-deleted-when-compute-finishes
  (let [a (ck/dataframe input)
        b (ck/dataframe input)
        path (out "join-clean.csv")]
    (ck/compute (ck/inner-join a b ["Employee"] ["Employee"]) 2 path)
    (is (< 1 (count (lines path))))
    (is (= [] (stray-tmp-files ".clojask/join")))))

(deftest join-swaps-larger-side-but-keeps-column-order
  (let [a-path (out "order-a.csv")
        b-path (out "order-b.csv")]
    (spit a-path "k,av\nx,a1\n")
    (spit b-path (str "k,bv,bw\nx,b1,b2\n" (str/join "" (repeat 20 "y,f1,f2\n"))))
    (doseq [in-memory [false true]]
      (testing (str "in-memory " in-memory)
        (let [a (ck/dataframe a-path)
              b (ck/dataframe b-path)
              inner (out "order-inner.csv")
              outer (out "order-outer.csv")
              selected (out "order-selected.csv")]
          (ck/compute (ck/inner-join a b ["k"] ["k"]) 1 inner :in-memory in-memory)
          (is (= ["1_k,1_av,2_k,2_bv,2_bw" "x,a1,x,b1,b2"] (lines inner)))
          (ck/compute (ck/outer-join a b ["k"] ["k"]) 1 outer :in-memory in-memory)
          (is (= "1_k,1_av,2_k,2_bv,2_bw" (first (lines outer))))
          (is (contains? (set (lines outer)) "x,a1,x,b1,b2"))
          (is (= 22 (count (lines outer))) "header, one match, twenty unmatched b rows")
          (ck/compute (ck/inner-join a b ["k"] ["k"]) 1 selected :select ["2_bv" "1_av"] :in-memory in-memory)
          (is (= ["2_bv,1_av" "b1,a1"] (lines selected))))))))

(deftest sort-orders-rows-by-the-trending-list
  (let [df (ck/dataframe input)
        path (out "sorted.csv")]
    (ck/set-type df "Salary" "double")
    (ck/sort df ["+" "Department" "-" "Salary"] path)
    (is (= ["Employee,EmployeeName,Department,Salary,UpdateDate"
            "7,Amy,11,50000,2020/11/26"
            "2,Bob,11,600,2020/12/01"
            "1,Alice,11,300,2020/12/12"
            "4,Daniel,12,1000,2020/12/05"
            "3,Carla,12,900,2020/12/03"
            "5,Evelyn,13,800,2020/12/03"
            "6,Ferdinand,21,700,2020/12/05"]
           (lines path)))))

(deftest reorder-col-keeps-operations-filters-and-types
  (let [df (ck/dataframe input)
        path (out "reorder.csv")
        order ["UpdateDate" "Employee" "EmployeeName" "Department" "Salary"]]
    (ck/set-type df "Employee" "int")
    (ck/operate df inc "Employee")
    (ck/filter df "Salary" #(> (Integer/parseInt %) 800))
    (ck/set-formatter df "Department" #(str "D" %))
    (ck/reorder-col df order)
    (is (= order (ck/get-col-names df)))
    (ck/compute df 1 path)
    (is (= ["UpdateDate,Employee,EmployeeName,Department,Salary"
            "2020/12/03,4,Carla,D12,900"
            "2020/12/05,5,Daniel,D12,1000"
            "2020/11/26,8,Amy,D11,50000"]
           (lines path)))
    (is (= {"UpdateDate" "2020/12/03" "Employee" "4" "EmployeeName" "Carla" "Department" "D12" "Salary" "900"}
           (update (first (ck/preview df 100 1 :format true)) "Employee" str)))))

(deftest delete-col-and-select-col-choose-the-output-columns
  (let [df (ck/dataframe input)
        path (out "delete.csv")]
    (ck/operate df #(str % "!") "EmployeeName")
    ;; a filter on a column that is deleted afterwards still applies
    (ck/filter df "Salary" #(> (Integer/parseInt %) 800))
    (ck/delete-col df ["Salary" "UpdateDate"])
    (is (= ["Employee" "EmployeeName" "Department"] (ck/get-col-names df)))
    (ck/compute df 1 path)
    (is (= ["Employee,EmployeeName,Department" "3,Carla!,12" "4,Daniel!,12" "7,Amy!,11"] (lines path)))
    (ck/select-col df ["Department" "Employee"])
    (is (= ["Employee" "Department"] (ck/get-col-names df)) "select-col keeps the current order")
    (ck/operate df str ["Employee" "Department"] "Both")
    (is (= ["Employee" "Department" "Both"] (ck/get-col-names df)) "a new column is added to the output")
    (is (= [["Employee" "Department" "Both"] ["3" "12" "312"] ["4" "12" "412"] ["7" "11" "711"]]
           (mapv vec (ck/compute df 1 nil))))
    (is (= {"Employee" "3" "Department" "12" "Both" "312"} (first (ck/preview df 100 1))))))

(deftest aggregate-can-use-a-deleted-column
  (let [df (ck/dataframe input)
        path (out "delete-aggregate.csv")]
    (ck/set-type df "Salary" "double")
    (ck/delete-col df ["Salary" "EmployeeName"])
    (ck/group-by df ["Department"])
    (ck/aggregate df gb/max ["Salary"] ["Salary-max"])
    (is (= 4 (count (ck/preview df 100 10))))
    (ck/compute df 1 path)
    (is (= 5 (count (lines path))) "header and four departments")
    (is (= "Department,Salary-max" (first (lines path))))))

(deftest joins-use-the-output-columns-of-each-side
  (doseq [in-memory [false true]]
    (testing (str "in-memory " in-memory)
      (let [a (ck/dataframe input)
            b (ck/dataframe "test/clojask/Employees-info-example.csv")
            path (out "join-view.csv")
            swapped (out "join-view-swapped.csv")
            selected (out "join-view-selected.csv")]
        (ck/select-col a ["Employee" "Salary"])
        (ck/reorder-col a ["Salary" "Employee"])
        ;; the join key of b is deleted from b's output, not from the join
        (ck/delete-col b ["Employee" "UpdateDate"])
        (let [j (ck/inner-join a b ["Employee"] ["Employee"])]
          (is (= ["1_Salary" "1_Employee" "2_EmployeeName" "2_DayOff"] (ck/get-col-names j)))
          (ck/compute j 1 path :in-memory in-memory)
          (is (= "1_Salary,1_Employee,2_EmployeeName,2_DayOff" (first (lines path))))
          (is (contains? (set (lines path)) "300,1,Alice,20")))
        (testing "the larger side second, so the join swaps internally"
          (let [j (ck/inner-join b a ["Employee"] ["Employee"])]
            (ck/compute j 1 swapped :in-memory in-memory)
            (is (= "1_EmployeeName,1_DayOff,2_Salary,2_Employee" (first (lines swapped))))
            (is (contains? (set (lines swapped)) "Alice,20,300,1"))
            (ck/compute j 1 selected :select ["2_Employee" "1_DayOff"] :in-memory in-memory)
            (is (contains? (set (lines selected)) "1,20"))))))))

(deftest column-functions-reject-bad-input
  (let [df (ck/dataframe input)
        names (ck/get-col-names df)]
    (is (thrown? TypeException (ck/delete-col df ["Nope"])))
    (is (thrown? TypeException (ck/select-col df ["Employee" "Nope"])))
    (is (thrown? OperationException (ck/delete-col df names)))
    (is (thrown? TypeException (ck/reorder-col df ["Employee"])))
    (is (thrown? TypeException (ck/reorder-col df (assoc names 1 "Employee"))))
    (is (= names (ck/get-col-names df)) "a rejected call changes nothing")
    (ck/delete-col df ["Salary"])
    (is (thrown? TypeException (ck/reorder-col df names)) "a deleted column is not output any more")
    (ck/group-by df ["Department"])
    (is (thrown? OperationException (ck/delete-col df ["Employee"])))
    (is (thrown? OperationException (ck/reorder-col df ["Department"])))))

(defn- excel-rows [path]
  (mapv vec (:data (read-excel path "Sheet1"))))

(deftest compute-writes-xlsx
  (testing "a simple compute"
    (let [df (ck/dataframe input)
          path (out "employees.xlsx")
          result (ck/compute df 1 path)]
      (is (= (ck/compute df 1 nil) (excel-rows path)))
      (is (= (ck/get-col-names df) (ck/get-col-names result)) "returns a dataframe read from the file")))
  (testing "a group-by aggregate, without header"
    (let [df (ck/dataframe input)
          path (out "groups.xlsx")]
      (ck/group-by df "Department")
      (ck/aggregate df gb/max "Salary")
      (ck/compute df 1 path :header false)
      (is (= (set (rest (ck/compute df 1 nil))) (set (excel-rows path))))))
  (testing "an Excel input written back to xlsx, which used the input's csv writer"
    (let [src (out "employees.xlsx")
          path (out "employees-copy.xlsx")]
      (ck/compute (ck/dataframe input) 1 src)
      (ck/compute (ck/dataframe (fn [] (read-excel src "Sheet1" :stat true))) 1 path)
      (is (= (excel-rows src) (excel-rows path))))))

(deftest compute-rejects-xls-output
  (let [path (out "employees.xls")]
    (spit path "keep me")
    (is (thrown-with-msg? TypeException #"\.xlsx only" (ck/compute (ck/dataframe input) 1 path)))
    (is (= "keep me" (slurp path)) "the existing file is not deleted")))
