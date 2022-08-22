(ns clojask.api.aggregate
  (:refer-clojure :exclude [max min count]))
"Contains implemented simple aggregation functions"

(def start)

(defn max
  [a b]
  (if (and (> (compare a b) 0) (not= a start))
    a
    b))

(defn min
  [a b]
  (if (and (< (compare a b) 0) (not= a start))
    a
    b))

(defn count
  [a b]
  (if (= a start)
    1
    (inc a)))

