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

    ; element-wise
    ; (set-type y "prccq" "double")
    ; (operate y (fn [val] (if val (+ val 10.0) 0.0)) "prccq")
    ; (time (compute y 4 "resources/test.csv" :select ["datadate" "tic" "prccq"] :exception false))
    
    ; row-wise
    ; (operate y str ["datadate" "tic"] "new-col")
    ; (time (compute y 4 "resources/test.csv" :select ["datadate" "tic" "prccq" "new-col"] :exception false))
    
    ; groupby-aggregate
    ; (set-type y "prccq" "double")
    ; (group-by y "tic")
    ; (aggregate y gb-aggre/max ["prccq"] ["prccq-max"])
    ; (time (compute y 4 "resources/test.csv" :select ["tic" "prccq-max"] :exception false))

    ; aggregate -> error?
    ; (set-type y "prccq" "double")
    ; (aggregate y aggre/max ["prccq"] ["prccq-max"])
    ; (time (compute y 4 "resources/test.csv" :select ["datadate" "tic" "prccq" "prccq-max"] :exception false))

    ;; CRSP

    ; element-wise
    ; (set-type y "PRC" "double")
    ; (operate y (fn [val] (if val (+ val 10.0) 0.0)) "prccq")
    ; (time (compute y 4 "resources/test.csv" :select ["date" "TICKER" "PRC"] :exception false))
    
    ; row-wise
    ; (operate y str ["PERMCO" "PERMNO"] "new-col")
    ; (time (compute y 4 "resources/test.csv" :select ["date" "TICKER" "PRC" "new-col"] :exception false))
    
    ; groupby-aggregate
    ; (set-type y "PRC" "double")
    ; (group-by y "tic")
    ; (aggregate y gb-aggre/max ["PRC"] ["PRC-max"])
    ; (time (compute y 4 "resources/test.csv" :select ["TICKER" "PRC-max"] :exception false))
    
    ;; obtain results
    (time (compute y 4 "resources/test.csv" :exception false))

    ;; join APIs

    ;(def x (dataframe "../clojure-datasets/data-CRSP.csv"))
    ;(def x (dataframe "resources/CRSP-extract.csv"))
    ;(def y (dataframe "../clojure-datasets/data-Compustat-lohi.csv"))

    ; (def output-df (left-join x y ["date" "TICKER"] ["datadate" "TICKER"] "date" "datadate"))
    ; (time (compute output-df :select ["1_date" "1_TICKER" "1_prccq" "2_datadate" "2_TICKER" "2_PRC"] 4 "resources/test.csv" :exception false))

    ; (def output-df (right-join x y ["date" "TICKER"] ["datadate" "TICKER"] "date" "datadate"))
    ; (time (compute output-df :select ["1_date" "1_TICKER" "1_prccq" "2_datadate" "2_TICKER" "2_PRC"] 4 "resources/test.csv" :exception false))

    ; (def output-df (inner-join x y ["date" "TICKER"] ["datadate" "TICKER"] "date" "datadate"))
    ; (time (compute output-df :select ["1_date" "1_TICKER" "1_prccq" "2_datadate" "2_TICKER" "2_PRC"] 4 "resources/test.csv" :exception false))

    ; (def output-df (rolling-join-forward x y ["TICKER"] ["tic"] "date" "datadate"))
    ; (time (compute output-df :select ["1_date" "1_TICKER" "1_prccq" "2_datadate" "2_TICKER" "2_PRC"] 4 "resources/test.csv" :exception false))

    )