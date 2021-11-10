(ns benchmark.core
    (:require [clojask.dataframe :refer :all]
              [clojure.core.async :as async]))
  
  (defn main
    []
    (def y (dataframe "../clojure-datasets/data-Compustat-lohi.csv")) ; 1.8 M dataset
    ;(def y (dataframe "../clojure-datasets/data-Compustat-x2.csv")) ; 3.6 M dataset
    ;(def y (dataframe "../clojure-datasets/data-CRSP.csv")) ; 80 M dataset

    ; =================== Change this part to test time taken ====================== ;

    ;; Compustat
    (set-type y "prccq" "double")
    ;(operate y - "prccq")
    ;(operate y str ["gvkey" "tic"] "new-col")
    ;(group-by y "tic")
    ;(aggregate y min ["prccq"] ["prccq-min"])

    ;; CRSP
    ;(set-type y "PRC" "double")
    ;(operate y - "PRC")
    ;(operate y str ["PERMCO" "PERMNO"] "new-col")
    ;(group-by y "TICKER")
    ;(aggregate y min ["PRC"] ["PRC-min"])
    
    ;; obtain results
    (time (compute y 4 "resources/test.csv" :exception false))

    ;; join APIs
    ;(def x (dataframe "../clojure-datasets/data-CRSP.csv"))
    ;(def x (dataframe "resources/CRSP-extract.csv"))
    ;(def y (dataframe "../clojure-datasets/data-Compustat-lohi.csv"))

    ;(time (left-join x y ["date" "TICKER"] ["datadate" "TICKER"] "date" "datadate" 4 "resources/test.csv" :exception false))
    ;(time (right-join x y ["date" "TICKER"] ["datadate" "TICKER"] "date" "datadate" 4 "resources/test.csv" :exception false))
    ;(time (inner-join x y ["date" "TICKER"] ["datadate" "TICKER"] "date" "datadate" 4 "resources/test.csv" :exception false))
    ;(time (rolling-join-forward x y ["TICKER"] ["tic"] "date" "datadate" 4 "resources/test.csv" :exception false))
    )