(ns clojask.utils)
"Implements some utilities function used in dataframe"

(defn func-name
  [tmp]
  (clojure.string/replace (clojure.string/replace tmp #"\$" "/") #"\@.+" ""))

(defn wrap-res
  [opr tmp-res]
  `(~opr ~tmp-res))

(defn lazy 
  [wrapped]
  (lazy-seq
   (if-let [s (seq wrapped)]
     (cons (first s) (lazy (rest s))))))

(defn eval-res
  [row opr-vec]
  (loop [res row oprs opr-vec]
      (let [opr (first oprs)
            rest (rest oprs)]
        (if (= (count oprs) 1)
          (opr res)
          (recur (opr res) rest)))))
