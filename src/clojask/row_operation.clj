(ns clojask.row-operation)
"Function used in catalog-user defined functions."

(defn add
  [new-key keys segment]
  {new-key (reduce + (select-keys segment keys))})

(defn inline-add
  [new-key keys segment]
  ;; new-key is the key of the key of the sum
  ;; segment is map of one row of the dataset
  ;; standard format:
  ;; {:id 1 :tic "AAPL" :price 37.5 ...}
  ;;
  ;; return is the segment adding the key element
  )

;; more to write

;; subtraction
(defn subtract
  [new-key keys segment]
  {new-key (reduce - (select-keys segment keys))})

;; multiplication
(defn multiply
  [new-key keys segment]
  {new-key (reduce * (select-keys segment keys))})

;; division
(defn division
  [new-key keys segment]
  {new-key (reduce / (select-keys segment keys))})

;; modulus
(defn modulus
  [new-key keys segment]
  {new-key (reduce mod (select-keys segment keys))})

;; exponentiation

;; logarithm

;; comparison

;; assignment