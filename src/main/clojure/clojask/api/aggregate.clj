(ns clojask.api.aggregate
  (:refer-clojure :exclude [max min sum count]))
"Contains implemented simple aggregation functions"

(def start)

;; (defn aggre-func
;;   "prev value could be start"
;;   [prev new])

;; single row aggregation functions

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

(defn sum
  [a b]
  (if (= a start)
    b
    (+ a b)))

(defn count
  [a b]
  (if (= a start)
    1
    (inc a)))

;; multi-row aggregation functions

(defn smallest3
  "return the smallest three entries"
  [a b]
  (cond
    (= start a) [b]
    :else (take 3 (sort (conj a b)))))

(defn smallestk
  "return the smallest k entries (the performance is better with smaller k)"
  [a b k]
  (cond
    (= start a) [b]
    :else (take k (sort (conj a b)))))

(defn largest3
  [a b]
  "return the largest three entries"
  (cond
    (= start a) [b]
    :else (take 3 (sort (fn [a b] (compare b a)) (conj a b)))))

(defn largestk
  [a b k]
  "return the largest three entries (the performance is better with smaller k)"
  (cond
    (= start a) [b]
    :else (take k (sort (fn [a b] (compare b a)) (conj a b)))))

