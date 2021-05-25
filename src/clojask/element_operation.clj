(ns clojask.element-operation)
"Function used in catalog-user defined functions."

(defn get
  [keys segment]
  ;; keys is a vector of keys to retrieve
  ;; segment is map of one row of the dataset
  ;; standard format:
  ;; {:id 1 :tic "AAPL" :price 37.5 ...}
  ;;
  ;; return is the map of the filtered map
  (select-keys segment keys))

;; more to write

;; For the below functions
;; Every one should have one inline version and a non-inline version



(defn neg
  [new-key key segment]
  {new-key (- (key segment))})

(defn inline-neg
  [key segment]
  ;; key is the key of the element to be negate
  ;; segment is map of one row of the dataset
  ;; standard format:
  ;; {:id 1 :tic "AAPL" :price 37.5 ...}
  ;;
  ;; return is the segment replacing the key element
  (assoc segment key (- (key segment))))

(defn square [segment]
  (update-in segment [:value]
    #(apply * % %)
    ))

;; help function 
(defn log-10 [n]
  (/ (Math/log n) (Math/log 10)))

(defn log [segment]
  (update-in segment [:value]
    #(apply log-10 %)
    ))

;; .
;; .
;; .