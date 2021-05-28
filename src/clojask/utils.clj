(ns clojask.utils)
"Implements some utilities function used in dataframe"

(defn func-name
  [tmp]
  (clojure.string/replace (clojure.string/replace tmp #"\$" "/") #"\@.+" ""))