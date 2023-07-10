(ns clojask.api.gb-aggregate
  (:require [clojask.api.aggregate :as agg])
  (:refer-clojure :exclude [max min sum count]))
"Contains the implemented function for group-by aggregation functions"

;; (defn aggre-func
;;   "function that can be applied on a collection"
;;   [list])

;; single row aggregation functions

(defn max
  [list]
  (reduce agg/max list))

(defn min
  [list]
  (reduce agg/min list))

(defn sum
  [list]
  (reduce + list))

(defn count
  [list]
  (clojure.core/count list))

(defn mean
  [list]
  (let [sum (apply + list)
        count (count list)]
    (if (pos? count)
      (/ sum count)
      0)))

(defn mode
  [list]
  (let [freqs (frequencies list)
        occurrences (clojure.core/group-by val freqs)
        modes (last (sort occurrences))
        modes (->> modes
                   val
                   (map key))]
    modes))

(defn median
  [list]
  (let [sorted (sort list)
        cnt (count sorted)
        halfway (quot cnt 2)]
    (if (odd? cnt)
      (nth sorted halfway)
      (let [bottom (dec halfway)
            bottom-val (nth sorted bottom)
            top-val (nth sorted halfway)]
        (mean [bottom-val top-val])))))

(defn sd
  [list]
  (let [avg (mean list)
        squares (for [x list]
                  (let [x-avg (- x avg)]
                    (* x-avg x-avg)))
        total (count list)]
    (if (= 1 total)
      0
      (-> (/ (apply + squares)
             (- total 1))
          (Math/sqrt)))))

(defn skew
  [list]
  (let [mean (mean list)
        median (median list)
        sd (sd list)]
    (* 3 (/ (- mean median) sd))))

;; multi-row aggregation functions

(defn smallest3
  "return the smallest 3 entries"
  [list]
  (reduce agg/smallest3 agg/start list))

(defn smallestk
  "return the smallest k entries (the performance is better with smaller k)"
  [list k]
  (reduce (fn [a b] (agg/smallestk a b k)) agg/start list))

(defn largest3
  "return the largest 3 entries"
  [list]
  (reduce agg/largest3 agg/start list))

(defn largestk
  "return the largest k entries (the performance is better with smaller k)"
  [list k]
  (reduce (fn [a b] (agg/largestk a b k)) agg/start list))

