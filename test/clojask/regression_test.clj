(ns regression-test
  "Computes that each exercised a bug fixed in 2.0.5. Starts Onyx."
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojask.dataframe :as ck]
            [clojask.api.aggregate :as agg]
            [clojask.api.gb-aggregate :as gb]))

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
