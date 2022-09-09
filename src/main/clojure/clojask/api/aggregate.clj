(ns clojask.api.aggregate
  (:refer-clojure :exclude [max min count]))
"Contains implemented simple aggregation functions"

(def start)

;; (defn aggre-func
;;   "prev value could be start"
;;   [prev new])

(defn max
  [a b]
  (if (or (= a start) (> (compare b a) 0))
    b
    a))

(defn min
  [a b]
  (if (or (= a start) (< (compare b a) 0))
    b
    a))

(defn count
  [a b]
  (if (= a start)
    1
    (inc a)))

