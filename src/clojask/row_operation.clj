(ns clojask.row-operation)
"Function used in catalog-user defined functions."

;; addition
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

;; multiplication

;; division

;; modulus

;; exponentiation

;; logarithm