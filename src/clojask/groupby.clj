(ns clojask.groupby
  (:require [clojure.java.io :as io]
            [clojure-csv.core :as csv]))
"contains the utility functions to group by and aggregate"

(defn compute-groupby
  "map the result to different files"
  [dataframe num-worker output-dir exception]
  )

(defn compute-aggregate
  "aggregate the output files to the final destination"
  [dateframe output-dir exp])

;; ;; the example of how to write a set of aggregate function
;; (defn min-pre
;;   []
;;   (def memo (atom 1)))

;; (defn min
;;   [row]
;;   (reset! memo (min (deref memo) row)))

;; (defn min-result
;;   []
;;   (deref memo))

(defn gen-groupby-filenames
  "internal function to generate files csv line with groupby key(s)"
  [msg groupby-keys]
  (def output-filename "./_grouped/")
  (doseq [groupby-key groupby-keys]
    (def output-filename (str output-filename "_" (name groupby-key) "-" (groupby-key msg))))
  (str output-filename ".csv"))

(defn output-groupby
  "internal function called by output when aggregation is applied"
  [msg groupby-keys]
  
  (let [output-filename (gen-groupby-filenames msg groupby-keys) ;; generate output filename
        groupby-wrtr (io/writer output-filename :append true)]
    ;; write as maps e.g. {:name "Tim", :salary 62, :tax 0.1, :bonus 12}
    (.write groupby-wrtr (str msg "\n"))

    ;; write as csv format e.g. Tim,62,0.1,12
    ;(.write groupby-wrtr (str (clojure.string/join "," (map msg (keys msg))) "\n"))

    ;; close writer
    (.close groupby-wrtr))

  ;; !! debugging
  ;(println (clojure.string/join "," (map msg (keys msg))))
  ;(println (apply str (map msg (keys msg))))
  )

(defn read-csv-seq
  "takes file name and reads data"
  [filename]
  (let [file (io/reader filename)]
    (->> file
         (line-seq)
         (map read-string)
         )))


(defn write-file
 [dir seq]
  (with-open [wtr (io/writer dir :append true)]
    (doseq [row seq]
    (if (not= row nil)
      (.write wtr (str row "\n"))))))

(defn internal-aggregate
  "aggregate one group use the function"
  [func out-dir groupby-keys keys & [new-keys]]
  (let [directory (clojure.java.io/file "./_grouped/")
        files (file-seq directory)]
    (doseq [file (rest files)]
      (write-file out-dir (func (read-csv-seq file) groupby-keys keys new-keys)))
    (doseq [file (rest (file-seq (clojure.java.io/file "./_grouped/")))]
      (io/delete-file file))
    "success"))

;; below are example aggregate functions

(defn aggre-min
  "get the min of some keys"
  [seq groupby-keys keys new-keys]
  (def _min (atom {}))
  (let [new-keys (if (= new-keys nil)
                   (vec (map (fn [_] (keyword (str "min(" _ ")"))) keys))
                   new-keys)
        a-old-keys (concat groupby-keys keys)
        a-new-keys (concat groupby-keys new-keys)]
    (assert (= (count keys) (count new-keys)) "number of new keys not equal to number of aggregation keys")
    (reset! _min (zipmap a-new-keys nil))
    ;; do one iteration to find the min
    (doseq [row seq]
      ;; (println row)
      (doseq [i (range (count a-old-keys))]
        (let [old-key (nth a-old-keys i)
              new-key (nth a-new-keys i)]
         (if (or (= (get (deref _min) new-key) nil) (<  (Integer/parseInt (get row old-key)) (get (deref _min) new-key)))
          (swap! _min assoc new-key (Integer/parseInt (get row old-key)))))))
    [(deref _min)]
    )
)

(defn aggre-max
  "get the max of some keys"
  [seq groupby-keys keys new-keys]
  (def _max (atom {}))
  (let [new-keys (if (= new-keys nil)
                   (vec (map (fn [_] (keyword (str "max(" _ ")"))) keys))
                   new-keys)
        a-old-keys (concat groupby-keys keys)
        a-new-keys (concat groupby-keys new-keys)]
    (assert (= (count keys) (count new-keys)) "number of new keys not equal to number of aggregation keys")
    (reset! _max (zipmap a-new-keys nil))
    ;; do one iteration to find the max
    (doseq [row seq]
      (doseq [i (range (count a-old-keys))]
        (let [old-key (nth a-old-keys i)
              new-key (nth a-new-keys i)]
         (if (or (= (get (deref _max) new-key) nil) (>  (Integer/parseInt (get row old-key)) (get (deref _max) new-key)))
          (swap! _max assoc new-key (Integer/parseInt (get row old-key)))))))
    [(deref _max)]
    )
)

(defn square [n] (* n n))

(defn mean [a] (/ (reduce + a) (count a)))

(defn standard-deviation
  [a]
  (let [mn (mean a)]
    (Math/sqrt
      (/ (reduce #(+ %1 (square (- %2 mn))) 0 a)
         (dec (count a))))))

;; !! check if new-keys are float cols
(defn aggre-sum
"get the sum of some keys"
[seq groupby-keys keys new-keys]
(def _sum (atom {}))
(let [new-keys (if (= new-keys nil)
                  (vec (map (fn [_] (keyword (str "sum(" _ ")"))) keys))
                  new-keys)]
  (assert (= (count keys) (count new-keys)) "number of new keys not equal to number of aggregation keys")
  (reset! _sum (zipmap (concat groupby-keys new-keys) nil))
  ;; do one iteration to find the sum
  (doseq [row seq]
    (doseq [i (range (count groupby-keys))]
      (let [old-key (nth keys i)
            new-key (nth new-keys i)]
          (swap! _sum assoc old-key (get row old-key))
          (swap! _sum assoc new-key (reduce + (doall (map #(Float/parseFloat (old-key %)) seq))))
        )))
  [(deref _sum)]
  )
)

;; !! to-debug
(defn aggre-sd
  "get the standard deviation (sd) of some keys"
  [seq groupby-keys keys new-keys]
  (def _sd (atom {}))
  (let [new-keys (if (= new-keys nil)
                   (vec (map (fn [_] (keyword (str "sd(" _ ")"))) keys))
                   new-keys)]
    (assert (= (count keys) (count new-keys)) "number of new keys not equal to number of aggregation keys")
    (reset! _sd (zipmap (concat groupby-keys new-keys) nil))
    ;; do one iteration to find the sum
    (doseq [row seq]
      (doseq [i (range (count groupby-keys))]
        (let [old-key (nth keys i)
              new-key (nth new-keys i)]
            (swap! _sd assoc old-key (get row old-key))
            (swap! _sd assoc new-key (reduce standard-deviation (doall (map #(Float/parseFloat (old-key %)) seq))))
          )))
    [(deref _sd)]
    )
)

(defn template
  "The template for aggregate functions"
  ;; seq: is a seq of maps (lazy) of the data from one of the file
  ;; groupby-keys: is a vector of the group by keys
  ;; old-keys: the columns to which this function applies
  ;; new-keys: the new-keys to replace the old-keys and receive the aggregation result
  [seq groupby-keys old-keys new-keys])
;; the return should be an vector of map (better lazy)

