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
  (assoc segment new-key (reduce + (map segment keys))))

(defn row-inline-add
  "returns :keys[1] {element1 + element2 (and so on)}"
  [keys segment]
  ;; new-key is the key of the key of the sum
  ;; segment is map of one row of the dataset
  ;; standard format:
  ;; {:id 1 :tic "AAPL" :price 37.5 ...}
  ;;
  ;; return is the segment adding the key element
  (row-add (first keys) keys segment))

;; subtraction
(defn row-subtract
  "returns :new-key {element1 - element2 (and so on)}"
  [new-key keys segment]
  (assoc segment new-key (reduce - (map segment keys))))

(defn row-inline-subtract
  "returns :keys[1] {element1 - element2 (and so on)}"
  [keys segment]
  (row-subtract (first keys) keys segment))

;; multiplication
(defn row-multiply
  "returns :new-key {element1 * element2 (and so on)}"
  [new-key keys segment]
  (assoc segment new-key (reduce * (map segment keys))))

(defn row-inline-multiply
  "returns :keys[1] {element1 * element2 (and so on)}"
  [keys segment]
  (row-multiply (first keys) keys segment))

;; division
(defn row-division
  "returns :new-key {element1 / element2 (and so on)}"
  [new-key keys segment]
  (assoc segment new-key (reduce / (map segment keys))))

(defn row-inline-division
  "returns :keys[1] {element1 / element2 (and so on)}"
  [keys segment]
  (row-division (first keys) keys segment))

;; modulus
(defn row-modulus
  "returns :new-key {element1 mod element2 (and so on)}"
  [new-key keys segment]
  (assoc segment new-key (reduce mod (map segment keys))))

(defn row-inline-modulus
  "returns :keys[1] {element1 mod element2 (and so on)}"
  [keys segment]
  (row-modulus (first keys) keys segment))

;; exponentiation
(defn row-exp
  "returns :new-key {element1 ^ element2 (and so on)}"
  [new-key keys segment]
  (assoc segment new-key (reduce math/expt (map segment keys))))

(defn row-inline-exp
  "returns :keys[1] {element1 ^ element2 (and so on)}"
  [keys segment]
  (row-exp (first keys) keys segment))

;; logarithm
(defn row-log-base
  "Helper function for log_<base>" 
  [n base]
  (/ (Math/log n) (Math/log base)))

(defn row-log
  "returns :new-key {element1^element2 (and so on)}"
  [new-key base keys segment]
  (assoc segment new-key (reduce row-log-base (map segment keys))))

(defn row-inline-log
  "returns :keys[1] {element1 - element2 (and so on)}"
  [keys segment]
  (row-log (first keys) keys segment))

;; comparison
(defn row-gr-than
  "returns :new-key {element1 > element2}" 
  [new-key keys segment]
  (assoc segment new-key (reduce > (map segment keys))))

(defn row-inline-gr-than
  "returns :key[1] {element1 > element2}" 
  [new-key keys segment]
  (row-gr-than (first keys) keys segment))

(defn row-geq-than
  "returns :new-key {element1 >= element2}" 
  [new-key keys segment]
  (assoc segment new-key (reduce >= (map segment keys))))

(defn row-inline-geq-than
  "returns :key[1] {element1 >= element2}" 
  [new-key keys segment]
  (row-geq-than (first keys) keys segment))

(defn row-le-than
  "returns :new-key {element1 < element2}" 
  [new-key keys segment]
  (assoc segment new-key (reduce < (map segment keys))))

(defn row-inline-le-than
  "returns :key[1] {element1 < element2}" 
  [new-key keys segment]
  (row-le-than (first keys) keys segment))

(defn row-leq-than
  "returns :new-key {element1 <= element2}" 
  [new-key keys segment]
  (assoc segment new-key (reduce <= (map segment keys))))

(defn row-inline-leq-than
  "returns :key[1] {element1 <= element2}" 
  [new-key keys segment]
  (row-leq-than (first keys) keys segment))

(defn row-equal
  "returns :new-key {element1 == element2}" 
  [new-key keys segment]
  (assoc segment new-key (reduce = (map segment keys))))

(defn row-inline-equal
  "returns :key[1] {element1 == element2}" 
  [new-key keys segment]
  (row-equal (first keys) keys segment))

(defn row-not-equal
  "returns :new-key {element1 != element2}" 
  [new-key keys segment]
  (assoc segment new-key (reduce not= (map segment keys))))

(defn row-not-equal
  "returns :key[1] {element1 != element2}" 
  [new-key keys segment]
  (row-not-equal (first keys) keys segment))
