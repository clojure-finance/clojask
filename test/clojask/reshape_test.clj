(ns reshape-test
  "melt and dcast round-trip the two fixtures: melt.csv is the wide
   layout and dcast.csv the long one."
  (:require [clojure.test :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojask.dataframe :as ck]
            [clojask.extensions.reshape :as rs]))

(io/make-parents "test/clojask/test_outputs/.keep")

(def wide-csv "test/clojask/melt.csv")
(def long-csv "test/clojask/dcast.csv")

(defn- lines [path]
  (vec (remove str/blank? (str/split-lines (slurp path)))))

(defn- same-table?
  "Same header, same rows in any order (group-by does not preserve order)."
  [expected actual]
  (and (= (first expected) (first actual))
       (= (sort (rest expected)) (sort (rest actual)))))

(deftest melt-wide-to-long
  (let [out "test/clojask/test_outputs/melt.csv"]
    (rs/melt (ck/dataframe wide-csv) out ["family_id" "age_mother"] ["dob_child1" "dob_child2" "dob_child3"])
    (is (= (first (lines long-csv)) (first (lines out))) "header names the id columns, measure and value")
    (is (same-table? (lines long-csv) (lines out)))))

(deftest melt-custom-names
  (let [out "test/clojask/test_outputs/melt-named.csv"]
    (rs/melt (ck/dataframe wide-csv) out ["family_id" "age_mother"] ["dob_child1" "dob_child2" "dob_child3"]
             :measure-name "child" :value-name "dob")
    (is (= "family_id,age_mother,child,dob" (first (lines out))))))

(deftest dcast-long-to-wide
  (let [out "test/clojask/test_outputs/dcast.csv"]
    (rs/dcast (ck/dataframe long-csv) out ["family_id" "age_mother"] "measure" "value" ["dob_child1" "dob_child2" "dob_child3"])
    (is (same-table? (lines wide-csv) (lines out)))))

(deftest dcast-renamed-value-columns
  (let [out "test/clojask/test_outputs/dcast-named.csv"]
    (rs/dcast (ck/dataframe long-csv) out ["family_id" "age_mother"] "measure" "value" ["dob_child1" "dob_child3"]
              :vals-name ["first" "third"])
    (is (= "family_id,age_mother,first,third" (first (lines out))))
    (is (contains? (set (lines out)) "3,26,2002-07-11,2007-09-02"))
    (is (contains? (set (lines out)) "2,27,1996-06-22,"))))

(deftest compute-header-collection
  (testing "compute :header with a collection replaces the column names"
    (let [out "test/clojask/test_outputs/header.csv"]
      (ck/compute (ck/dataframe wide-csv) 1 out :select ["family_id" "age_mother"] :header ["id" "age"])
      (is (= "id,age" (first (lines out))))
      (is (= 6 (count (lines out)))))))
