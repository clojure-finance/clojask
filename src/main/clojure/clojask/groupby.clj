(ns clojask.groupby
  (:require [clojure.java.io :as io]
            [clojure.core.async :as async]))
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
  [dist msg groupby-keys key-index formatters]
  ;; (def output-filename dist)
  ;; (doseq [groupby-key groupby-keys]
  ;;   (def output-filename (str output-filename "_" (name groupby-key) "-" (nth msg (get key-index groupby-key)))))
  ;; (str output-filename ".csv")
  (let [index groupby-keys
        ;; (map (fn [_] (get key-index _)) groupby-keys)
        val (mapv (fn [_] 
                    (let [func (nth _ 0)
                          _ (nth _ 1)]
                     (if func
                       (func (nth msg _))
                       (if-let [formatter (get formatters _)]
                         (formatter (nth msg _))
                         (nth msg _))))) 
                  index)]
    (str dist val)))

(defn output-groupby
  "internal function called by output when aggregation is applied"
  [dist msg groupby-keys key-index formatter]
  ;; msg this time is a vector

  ;; key-index contains the one to one correspondence of key value to index value, it is a map
  ;; eg "Salary" -> 3
  ;; (spit "resources/debug.txt" (str msg "\n" key-index) :append true)
  (let [output-filename (gen-groupby-filenames dist msg groupby-keys key-index formatter) ;; generate output filename
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

(defn internal-aggregate-write
  "called by child thread function"
  [func out-dir groupby-keys keys file & [new-keys]]
  (async/thread
    (write-file out-dir (func (read-csv-seq file) groupby-keys keys new-keys))))

(defn internal-aggregate
  "aggregate one group use the function"
  [func out-dir key-index groupby-keys keys & [new-keys]]
  (let [directory (clojure.java.io/file "./_clojask/grouped/")
        files (file-seq directory)]
    (doseq [file (rest files)]
      ;; w/o multi-threading
      (write-file out-dir (func (read-csv-seq file) groupby-keys keys new-keys key-index))
      ;; multi-threading
      ;(async/go (async/<! (internal-aggregate-write func out-dir groupby-keys keys file [new-keys])))
      (io/delete-file file true)
      )
    (doseq [file (rest (file-seq (clojure.java.io/file "./_grouped/")))]
       (io/delete-file file))
    "success"))



;; below are example aggregate functions

(defn aggre-min
  "get the min of some keys"
  [seq groupby-keys keys new-keys key-index]
  (let [_min (atom [])] 
    (assert (= (count keys) (count new-keys)) "number of new keys not equal to number of aggregation keys")
    (doseq [groupby-key keys]
      (let [vec-index (get key-index groupby-key)] ;; get index number in vector
        ;; initialise min with first value
        (swap! _min assoc (.indexOf keys groupby-key) (nth (first seq) vec-index))
        (doseq [row seq]
          ;; do one iteration to find the min
          (let [curr-val (Integer/parseInt (nth row vec-index))
                curr-min (Integer/parseInt (nth (deref _min) (.indexOf keys groupby-key)))]
            (if (< curr-val curr-min)
              (swap! _min assoc (.indexOf keys groupby-key) (nth row vec-index))))
        )))
    ;(println (deref _min))
    [(deref _min)]
    )
)

(defn aggre-max
  "get the max of some keys"
  [seq groupby-keys keys new-keys key-index]
  (let [_max (atom [])] 
    (assert (= (count keys) (count new-keys)) "number of new keys not equal to number of aggregation keys")
    (doseq [groupby-key keys]
      (let [vec-index (get key-index groupby-key)] ;; get index number in vector
        ;; initialise max with first value
        (swap! _max assoc (.indexOf keys groupby-key) (nth (first seq) vec-index))
        (doseq [row seq]
          ;; do one iteration to find the min
          (let [curr-val (Integer/parseInt (nth row vec-index))
                curr-max (Integer/parseInt (nth (deref _max) (.indexOf keys groupby-key)))]
            (if (> curr-val curr-max)
              (swap! _max assoc (.indexOf keys groupby-key) (nth row vec-index))))
        )))
    ;(println (deref _max))
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

;; !! check if new-keys are float/int cols
(defn aggre-sum
  "get the sum of some keys"
  [seq groupby-keys keys new-keys key-index]
    (let [_sum (atom [])] 
      (assert (= (count keys) (count new-keys)) "number of new keys not equal to number of aggregation keys")
      (doseq [groupby-key keys]
        (let [vec-index (get key-index groupby-key)] ;; get index number in vector
          ;; initialise max with zero
          (swap! _sum assoc (.indexOf keys groupby-key) 0.0)
          (doseq [row seq]
            ;; do one iteration to get sum
            (let [curr-val (Float/parseFloat (nth row vec-index))
                  curr-sum (nth (deref _sum) (.indexOf keys groupby-key))]
                (swap! _sum assoc (.indexOf keys groupby-key) (+ curr-val curr-sum)))
          )))
      (println (deref _sum))
      [(deref _sum)]
      )
)

;; !! check if new-keys are float/int cols
(defn aggre-avg
  "get the average of some keys"
  [seq groupby-keys keys new-keys key-index]
    (let [_avg (atom [])] 
      (assert (= (count keys) (count new-keys)) "number of new keys not equal to number of aggregation keys")
      (doseq [groupby-key keys]
        (let [vec-index (get key-index groupby-key) ;; get index number in vector
              avg-value (/ (reduce + (doall (map #(Float/parseFloat (nth % vec-index)) seq))) (count seq))] 
          (swap! _avg assoc (.indexOf keys groupby-key) avg-value)
          ))
      ;(println (deref _avg))
      [(deref _avg)]
      )
)

;; !! check if new-keys are float/int cols
(defn aggre-sd
  "get the standard deviation (sd) of some keys"
  [seq groupby-keys keys new-keys key-index]
    (let [_sd (atom [])] 
      (assert (= (count keys) (count new-keys)) "number of new keys not equal to number of aggregation keys")
      (doseq [groupby-key keys]
        (let [vec-index (get key-index groupby-key) ;; get index number in vector
              sd-value (standard-deviation (doall (map #(Float/parseFloat (nth % vec-index)) seq)))] 
          (swap! _sd assoc (.indexOf keys groupby-key) sd-value)
          ))
      ;(println (deref _sd))
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

