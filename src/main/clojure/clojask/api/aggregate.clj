(ns clojask.api.aggregate)
"Contains implemented simple aggregation functions"

(def start)

(defn max
  [a b]
  (if (and (not= b start) (< (compare a b) 0))
    b
    a))

(defn min
  [a b]
  (if (and (not= b start) (> (compare a b) 0))
    b
    a))

