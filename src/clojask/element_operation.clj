(ns clojask.element-operation)
"Function used in catalog-user defined functions."

(defn get-key
  [keys segment]
  ;; keys is a vector of keys to retrieve
  ;; segment is map of one row of the dataset
  ;; standard format:
  ;; {:id 1 :tic "AAPL" :price 37.5 ...}
  ;;
  ;; return is the map of the filtered map
  (select-keys segment keys))

(defn assign
  "returns :new-key {element}"
  [n key segment]
  (assoc segment key n))
;; more to write

;; For the below functions
;; Every one should have one inline version and a non-inline version

;; negation
(defn neg
  "returns :new-key {- element}"
  [new-key key segment]
  ;(update-in segment [key] #(apply * -1 %))
  ;(fn [segment] {new-key (* (key segment) -1)}))
  ;{new-key (* (key segment) -1)})
  ;; (let [new-segment (assoc segment new-key (* (key segment) -1))]
  ;;   new-segment
  ;; )
  (assoc segment new-key (* (key segment) -1))
  )

(defn inline-neg
  "returns :key {- element}"
  [key segment]
  ;; key is the key of the element to be negate
  ;; segment is map of one row of the dataset
  ;; standard format:
  ;; {:id 1 :tic "AAPL" :price 37.5 ...}
  ;;
  ;; return is the segment replacing the key element
  ;(assoc segment key 0))
  (assoc segment key (* (key segment) -1)))

;; addition
(defn add-const
  "returns :new-key {element + const}"
  [new-key const key segment]
  {new-key (+ (key segment) const)})

(defn inline-add-const
  "returns :key {element + const}"
  [const key segment]
  (assoc segment key (+ (key segment) const)))

;; subtraction
(defn subtract-const
  "returns :new-key {element - const}"
  [new-key const key segment]
  {new-key (- (key segment) const)})

(defn inline-subtract-const
  "returns :key {element - const}"
  [const key segment]
  (assoc segment key (- (key segment) const)))

;; multiplication
(defn multiply-const
  "returns :new-key {element * const}"
  [new-key const key segment]
  {new-key (* (key segment) const)})

(defn inline-multiply-const
  "returns :key {element * const}"
  [const key segment]
  (assoc segment key (* (key segment) const)))

;; divison
(defn divide-const
  "returns :new-key {element / const}"
  [new-key const key segment]
  {new-key (/ (key segment) const)})

(defn inline-divide-const
  "returns :key {element / const}"
  [const key segment]
  (assoc segment key (/ (key segment) const)))

;; modulus
(defn modulus-const
  "returns :new-key {element mod const}"
  [new-key const key segment]
  {new-key (mod (key segment) const)})

(defn inline-modulus-const
  "returns :key {element mod const}"
  [const key segment]
  (assoc segment key (mod (key segment) const)))

(defn square 
  "returns :new-key {element^2}"
  [new-key key segment]
  {new-key (* (key segment) (key segment))})

(defn inline-square
  "returns :key {element^2}"
  [key segment]
  (assoc segment key (* (key segment) (key segment))))

;; exponentiation
(defn exp
  "returns :new-key {element^exponent}"
  [new-key exponent key segment]
  {new-key (Math/pow (key segment) exponent)})

(defn inline-exp
  "returns :key {element^exponent}"
  [exponent key segment]
  (assoc segment key (Math/pow (key segment) exponent)))
  
;; logarithm
(defn log-base
  "Helper function for log_<base>" 
  [n base]
  (/ (Math/log n) (Math/log base)))

(defn log 
  "returns :new-key {log_base(element)}"
  [new-key base key segment]
  {new-key (log-base (key segment) base)})

(defn inline-log
  "returns :key {log_base(element)}"
  [base key segment]
  (assoc segment key (log-base (key segment) base)))

;; comparison
(defn gr-than
  "returns :new-key {element > n}" 
  [new-key n key segment]
  {new-key (> (key segment) n)})

(defn geq-than
  "returns :new-key {element >= n}" 
  [new-key n key segment]
  {new-key (>= (key segment) n)})

(defn le-than
  "returns :new-key {element < n}" 
  [new-key n key segment]
  {new-key (< (key segment) n)})

(defn leq-than
  "returns :new-key {element <= n}" 
  [new-key n key segment]
  {new-key (<= (key segment) n)})

(defn equal
  "returns :new-key {element == n}" 
  [new-key n key segment]
  {new-key (= (key segment) n)})

(defn not-equal
  "returns :new-key {element != n}" 
  [new-key n key segment]
  {new-key (not= (key segment) n)})