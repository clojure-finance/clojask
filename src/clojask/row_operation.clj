(ns clojask.row-operation
  (:require [clojure.math.numeric-tower :as math])
  )
"Function used in catalog-user defined functions."

;; copy values from key to new-key
(defn row-copy
  "returns :new-key {element}"
  [new-key key segment]
  (assoc segment new-key (key segment)))

;; addition
(defn row-add
  "returns :new-key {element1 + element2 (and so on)}"
  [new-key keys segment]
  (assoc segment new-key (reduce + (select-keys segment keys))))

(defn row-inline-add
  "returns :keys[1] {element1 + element2 (and so on)}"
  [new-key keys segment]
  ;; new-key is the key of the key of the sum
  ;; segment is map of one row of the dataset
  ;; standard format:
  ;; {:id 1 :tic "AAPL" :price 37.5 ...}
  ;;
  ;; return is the segment adding the key element
  )

;; subtraction
(defn row-subtract
  "returns :new-key {element1 - element2 (and so on)}"
  [new-key keys segment]
  {new-key (reduce - (select-keys segment keys))})

;; multiplication
(defn row-multiply
  "returns :new-key {element1 * element2 (and so on)}"
  [new-key keys segment]
  {new-key (reduce * (select-keys segment keys))})

;; division
(defn row-division
  "returns :new-key {element1 / element2 (and so on)}"
  [new-key keys segment]
  {new-key (reduce / (select-keys segment keys))})

;; modulus
(defn row-modulus
  "returns :new-key {element1 mod element2 (and so on)}"
  [new-key keys segment]
  {new-key (reduce mod (select-keys segment keys))})

;; exponentiation
(defn row-exp
  "returns :new-key {element1^element2 (and so on)}"
  [new-key keys segment]
  (assoc segment new-key (reduce math/expt (select-keys segment keys))))

;; logarithm
(defn row-log-base
  "Helper function for log_<base>" 
  [n base]
  (/ (Math/log n) (Math/log base)))

(defn row-log
  "returns :new-key {element1^element2 (and so on)}"
  [new-key base keys segment]
  {new-key (reduce row-log-base (select-keys segment keys) base)})

;; comparison
(defn row-gr-than
  "returns :new-key {element1 > element2}" 
  [new-key keys segment]
  {new-key (reduce > (select-keys segment keys))})

(defn row-geq-than
  "returns :new-key {element1 >= element2}" 
  [new-key keys segment]
  {new-key (reduce >= (select-keys segment keys))})

(defn row-le-than
  "returns :new-key {element1 < element2}" 
  [new-key keys segment]
  {new-key (reduce < (select-keys segment keys))})

(defn row-leq-than
  "returns :new-key {element1 <= element2}" 
  [new-key keys segment]
  {new-key (reduce <= (select-keys segment keys))})

(defn row-equal
  "returns :new-key {element1 == element2}" 
  [new-key keys segment]
  {new-key (reduce = (select-keys segment keys))})

(defn row-not-equal
  "returns :new-key {element1 != element2}" 
  [new-key keys segment]
  {new-key (reduce not= (select-keys segment keys))})
