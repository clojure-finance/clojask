(ns examples.reshape
  (:require [clojask.dataframe :as cj]
            [clojask.extensions.reshape :refer [melt dcast]]
            [clojask.extensions.bind :refer [rbind-csv]]))

(defn main
  []
  ;; enhanced melt
  (def x (cj/dataframe "/Users/lyc/Desktop/RA clojure/clojask/examples/enhanced reshape/melt.csv"))
  ;; some operations to x
  (cj/rename-col x "dob_child1" "child1")
  (cj/rename-col x "dob_child2" "child2")
  (cj/rename-col x "dob_child3" "child3")
  (melt x "/Users/lyc/Desktop/RA clojure/clojask/examples/enhanced reshape/1.csv" ["family_id" "age_mother"] ["child1" "child2" "child3"] :measure-name "child" :value-name "dob")
  ;; x and y are from the same source
  (def y (cj/dataframe "/Users/lyc/Desktop/RA clojure/clojask/examples/enhanced reshape/melt.csv"))
  (cj/rename-col x "gender_child1" "child1")
  (cj/rename-col x "gender_child2" "child2")
  (cj/rename-col x "gender_child3" "child3")
  (melt x "/Users/lyc/Desktop/RA clojure/clojask/examples/enhanced reshape/2.csv" ["family_id" "age_mother"] ["child1" "child2" "child3"] :measure-name "child" :value-name "gender")
  (def z (cbind-csv "/Users/lyc/Desktop/RA clojure/clojask/examples/enhanced reshape/1.csv" "/Users/lyc/Desktop/RA clojure/clojask/examples/enhanced reshape/2.csv"))
  ;; you can rename the column names of z here
  ;; skipped
  (cj/compute z 8 "/Users/lyc/Desktop/RA clojure/clojask/examples/enhanced reshape/melt_result.csv" :select ["family_id1" "age_mother1" "child1" "dob" "gender"])

  ;; enhanced dcast
  (def a (cj/dataframe "/Users/lyc/Desktop/RA clojure/clojask/examples/enhanced reshape/dcast.csv"))
  (def dob (dcast a "/Users/lyc/Desktop/RA clojure/clojask/examples/enhanced reshape/1.csv" ["family_id" "age_mother"] "child" "dob" ["1" "2" "3"] :vals-name ["child1" "child2" "child3"]))
  (def b (cj/dataframe "/Users/lyc/Desktop/RA clojure/clojask/examples/enhanced reshape/dcast.csv"))
  (def gender (dcast b "/Users/lyc/Desktop/RA clojure/clojask/examples/enhanced reshape/2.csv" ["family_id" "age_mother"] "child" "gender" ["1" "2" "3"] :vals-name ["child1" "child2" "child3"]))
  (def res (cj/inner-join dob gender ["family_id" "age_mother"] ["family_id" "age_mother"] :col-prefix ["dob" "gender"]))
  ;; you can rename the column names of res here
  ;; skipped
  (cj/compute res 8 "/Users/lyc/Desktop/RA clojure/clojask/examples/enhanced reshape/dcast_result.csv" :exclude ["dob_family_id" "dob_age_mother"]))