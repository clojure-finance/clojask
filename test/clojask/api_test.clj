(ns api-test
  "Argument validation in the public API. Nothing here starts Onyx, so
   these run in well under a second."
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojask.dataframe :as ck]
            [clojask.api.aggregate :as agg]
            [clojask.api.gb-aggregate :as gb]
            [clojask-io.input :as cio])
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
    (is (= 2.0 (gb/median [1 2 3])))
    (is (= 0.0 (gb/sd [7]))))
  (testing "skew is NaN instead of throwing when the standard deviation is zero"
    (is (Double/isNaN (gb/skew [5])))
    (is (Double/isNaN (gb/skew [2.0 2.0 2.0])))
    (is (Double/isNaN (gb/skew [0.1 0.1 0.1])) "sd is 1.7e-17 here, not zero")))

(deftest joins-reject-unknown-key-columns
  (let [a (ck/dataframe input)
        b (ck/dataframe input-b)]
    (is (thrown-with-msg? TypeException #"non-existent" (ck/inner-join a b ["Nope"] ["Employee"])))
    (is (thrown-with-msg? TypeException #"non-existent" (ck/left-join a b ["Employee"] ["Nope"])))
    (is (thrown-with-msg? TypeException #"non-existent" (ck/rolling-join-forward a b ["Nope"] ["Employee"] "UpdateDate" "UpdateDate")))))

(deftest rename-col-refuses-existing-name
  (let [df (ck/dataframe input)
        names (ck/get-col-names df)]
    (is (thrown-with-msg? TypeException #"already exists" (ck/rename-col df "Employee" "Salary")))
    (is (= names (ck/get-col-names df)))))

(deftest failed-set-type-rolls-back-parser-and-formatter
  (let [df (ck/dataframe (fn [] [["d"] ["not a date"]]))]
    (is (thrown? OperationException (ck/set-type df "d" "date:dd/MM/yyyy")))
    (is (= [{"d" "not a date"}] (ck/preview df 10 10)) "the column is usable again")))

(deftest print-df-marks-truncation
  (let [df (ck/dataframe input)]
    (is (str/includes? (with-out-str (ck/print-df df 100 3)) "...") "seven rows, three shown")
    (is (not (str/includes? (with-out-str (ck/print-df df 100 7)) "...")) "all seven shown")
    (is (= 3 (count (re-seq #"(?m)^\|\s+\d+ \|" (with-out-str (ck/print-df df 100 3))))) "three data rows")))

(deftest formatted-preview-drops-filtered-rows
  (let [df (ck/dataframe input)]
    (ck/set-type df "Salary" "double")
    (ck/filter df "Salary" (fn [salary] (<= salary 800)))
    (is (= 4 (count (ck/preview df 100 100 :format true))))))

(deftest compute-validates-num-worker
  (testing "checked for every dataframe type, not only group-by/aggregate"
    (let [df (ck/dataframe input)]
      (is (thrown-with-msg? TypeException #"should be an integer" (ck/compute df "4" nil)))
      (is (thrown-with-msg? OperationException #"at least 1" (ck/compute df 0 nil)))
      (is (thrown-with-msg? OperationException #"Max number of worker nodes" (ck/compute df 9 nil))))))

(deftest compute-rejects-unknown-select-names
  (let [a (ck/dataframe input)
        b (ck/dataframe input-b)]
    (testing "simple dataframe"
      (is (thrown-with-msg? TypeException #"NoSuchColumn" (ck/compute a 1 nil :select ["NoSuchColumn"]))))
    (testing "a single name not wrapped in a collection"
      (is (thrown-with-msg? TypeException #"NoSuchColumn" (ck/compute a 1 nil :select "NoSuchColumn"))))
    (testing "aggregated dataframe used to hit nth with index -1"
      (let [d (ck/dataframe input)]
        (ck/aggregate d agg/max ["Salary"])
        (is (= ["max(Salary)"] (ck/get-col-names d)))
        (is (thrown-with-msg? TypeException #"NoSuchColumn" (ck/compute d 1 nil :select ["NoSuchColumn"])))))
    (testing "joined dataframe used to hit an IndexOutOfBoundsException"
      (let [j (ck/inner-join a b ["Employee"] ["Employee"])]
        (is (thrown-with-msg? TypeException #"NoSuchColumn" (ck/compute j 1 nil :select ["NoSuchColumn"])))))))

(deftest failed-compute-validation-leaves-output-file-alone
  (let [df (ck/dataframe input)
        out "test/clojask/test_outputs/precious.csv"]
    (io/make-parents out)
    (spit out "precious\n")
    (is (thrown? AssertionError (ck/compute df 1 out :select [])))
    (is (thrown? TypeException (ck/compute df 1 out :select ["NoSuchColumn"])))
    (is (thrown? OperationException (ck/compute df 9 out)))
    (is (= "precious\n" (slurp out)) "failed validation must not delete the output file")
    (io/delete-file out true)))

(deftest aggregate-checks-key-counts
  (let [df (ck/dataframe input)]
    (is (thrown-with-msg? TypeException #"number of new keys"
                          (ck/aggregate df agg/max ["Salary" "Salary"] "just-one-name")))
    (is (thrown-with-msg? TypeException #"number of new keys"
                          (ck/aggregate df agg/max ["Salary"] ["a" "b"])))
    (is (= ["Employee" "EmployeeName" "Department" "Salary" "UpdateDate"] (ck/get-col-names df))
        "failed aggregate leaves the dataframe unchanged")))

(deftest mode-ties-are-deterministic
  (is (= [5] (gb/mode [5 5 1])))
  (is (= [1 2] (gb/mode [1 2 1 2 3])))
  (is (= [1.0 2.0] (gb/mode [2.0 1.0 2.0 1.0 3.0])) "used to come back in hash order")
  (is (= ["a" "b"] (gb/mode ["b" "a" "b" "a"]))))

(deftest dataframe-and-preview-close-abandoned-readers
  (testing "construction, preview and a failing errorPredetect close every reader they open"
    (let [opened (atom 0)
          closed (atom 0)
          orig cio/read-file]
      (with-redefs [cio/read-file
                    (fn [& args]
                      (let [res (apply orig args)]
                        (swap! opened inc)
                        (update res :close (fn [close] (fn [] (swap! closed inc) (when close (close)))))))]
        (let [df (ck/dataframe input)]
          (is (pos? @opened) "construction reads the input")
          (is (= @opened @closed) "construction closes its readers")
          (ck/preview df 5 5)
          (is (= @opened @closed) "preview closes its reader")
          (is (thrown? OperationException
                       (ck/filter df "Salary" (fn [_] (throw (Exception. "boom"))))))
          (is (= @opened @closed) "a failing errorPredetect closes its reader"))))))
