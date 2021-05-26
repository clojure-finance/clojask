(ns clojask.row-operation)
"Function used in catalog-user defined functions."

;; assignment
(defn assign
  "returns :new-key {element}"
  [new-key key segment]
  {new-key (key segment)})

;; addition
(defn add
  "returns :new-key {element1 + element2 (and so on)}"
  [new-key keys segment]
  {new-key (reduce + (select-keys segment keys))})

(defn inline-add
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
(defn subtract
  "returns :new-key {element1 - element2 (and so on)}"
  [new-key keys segment]
  {new-key (reduce - (select-keys segment keys))})

;; multiplication
(defn multiply
  "returns :new-key {element1 * element2 (and so on)}"
  [new-key keys segment]
  {new-key (reduce * (select-keys segment keys))})

;; division
(defn division
  "returns :new-key {element1 / element2 (and so on)}"
  [new-key keys segment]
  {new-key (reduce / (select-keys segment keys))})

;; modulus
(defn modulus
  "returns :new-key {element1 mod element2 (and so on)}"
  [new-key keys segment]
  {new-key (reduce mod (select-keys segment keys))})

;; exponentiation
(defn exp
  "returns :new-key {element1^element2 (and so on)}"
  [new-key keys segment]
  {new-key (reduce Math/pow (select-keys segment keys))})

;; logarithm
(defn log-base
  "Helper function for log_<base>" 
  [n base]
  (/ (Math/log n) (Math/log base)))

(defn log
  "returns :new-key {element1^element2 (and so on)}"
  [new-key base keys segment]
  {new-key (reduce Math/log (select-keys segment keys) base)})

;; comparison
(defn gr-than
  "returns :new-key {element1 > element2}" 
  [new-key keys segment]
  {new-key (reduce > (select-keys segment keys))})

(defn geq-than
  "returns :new-key {element1 >= element2}" 
  [new-key keys segment]
  {new-key (reduce >= (select-keys segment keys))})

(defn le-than
  "returns :new-key {element1 < element2}" 
  [new-key keys segment]
  {new-key (reduce < (select-keys segment keys))})

(defn leq-than
  "returns :new-key {element1 <= element2}" 
  [new-key keys segment]
  {new-key (reduce <= (select-keys segment keys))})

(defn equal
  "returns :new-key {element1 == element2}" 
  [new-key keys segment]
  {new-key (reduce = (select-keys segment keys))})

(defn not-equal
  "returns :new-key {element1 != element2}" 
  [new-key keys segment]
  {new-key (reduce not= (select-keys segment keys))})