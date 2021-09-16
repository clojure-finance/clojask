(ns examples.timezone
    (:require [clojask.DataFrame :as clojask]
              [clojure.core.async :as async]))
  
  (def main
    []
    (def x (dataframe "resources/Employees-large.csv"))
    (def y (dataframe "resources/Employees.csv"))

    ;; create a thread for each operation
    (async/thread (set-type x "double" "Department"))
    (async/thread (set-type y "double" "Department"))

    (time (left-join x y ["Employee"] ["Employee"] 4 "resources/test.csv" :exception false))
    )