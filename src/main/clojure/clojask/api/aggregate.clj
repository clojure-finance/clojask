(ns clojask.api.aggregate)
"Contains implemented simple aggregation functions"

(def start)

(defn max
  [a b]
  (if (and (not= a start) (> (compare a b) 0))
    a
    b))

(defn min
  [a b]
  (if (and (not= a start) (< (compare a b) 0))
    a
    b))

