(ns examples.timezone
  (:require [clojask.DataFrame :as clojask]))

(defn timezone-parser
  "the input is a datetime string with timezone identifier as suffix"
  [time-string]
  )

(defn timezone-formatter
  "the input is a vector, the first element is a date object, the second is the timezone string"
  [time-vec]
  )

(def main
  []
  (def df (clojask/dataframe "sales.csv"))
  )