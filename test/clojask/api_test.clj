(ns api-test
  "Argument validation in the public API. Nothing here starts Onyx, so
   these run in well under a second."
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojask.dataframe :as ck]
            [clojask.api.gb-aggregate :as gb])
  (:import [com.clojask.exception OperationException TypeException]))

(def input "test/clojask/Employees-example.csv")
(def input-b "test/clojask/Employees-info-example.csv")

(deftest compute-refuses-input-path-as-output
  (testing "compute deletes the output file first, so it must refuse its own input"
    (let [df (ck/dataframe input)
          size (.length (io/file input))]
      (is (thrown? OperationException (ck/compute df 1 input)))
      (is (thrown? OperationException (ck/compute df 1 (str "./" input))))
      (is (= size (.length (io/file input))) "input file is untouched"))))

(deftest joined-compute-refuses-either-input-path-as-output
  (let [a (ck/dataframe input)
        b (ck/dataframe input-b)
        joined (ck/inner-join a b ["Employee"] ["Employee"])]
    (is (thrown? OperationException (ck/compute joined 1 input)))
    (is (thrown? OperationException (ck/compute joined 1 input-b)))))

(deftest path-clash-check-tolerates-missing-paths
  (let [from-fn (ck/dataframe (fn [] [["a" "b"] ["1" "2"]]))
        from-file (ck/dataframe input)]
    (is (nil? (.checkInputPathClash from-fn "anything.csv")))
    (is (nil? (.checkInputPathClash from-fn nil)))
    (is (nil? (.checkInputPathClash from-file nil)) "in-memory compute has no output path")))

(deftest set-type-validates-its-arguments
  (let [df (ck/dataframe input)]
    (is (thrown-with-msg? TypeException #"No such type: bogus" (ck/set-type df "Salary" "bogus")))
    (is (thrown-with-msg? TypeException #"non-existent column" (ck/set-type df "NoSuchColumn" "double")))
    (is (identical? df (ck/set-type df "Salary" "double")))
    (is (identical? df (ck/set-type df "UpdateDate" "date:yyyy/MM/dd")))))

(deftest if-header-false-generates-column-names
  (is (= ["Col_1" "Col_2" "Col_3" "Col_4" "Col_5"]
         (ck/get-col-names (ck/dataframe input :if-header false)))))

(deftest gb-aggregate-statistics
  (testing "mean and median return doubles, not ratios"
    (is (= 1.5 (gb/mean [1 2])))
    (is (= 2.5 (gb/median [1 2 3 4])))
    (is (= 2 (gb/median [1 2 3]))))
  (testing "skew is NaN instead of throwing when the standard deviation is zero"
    (is (Double/isNaN (gb/skew [5])))
    (is (Double/isNaN (gb/skew [2.0 2.0 2.0])))))
