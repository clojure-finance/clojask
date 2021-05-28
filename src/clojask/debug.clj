(ns clojask.debug
  (:require [clojask.DataFrame :refer :all]))
"For debugging purposes only, will not be used in production."

(defn -main
  []
  (def x (dataframe "/Users/lyc/Desktop/RA clojure/Employees.csv"))
  (.compute x "./resources/test.csv"))