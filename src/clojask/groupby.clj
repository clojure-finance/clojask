(ns clojask.groupby)
"contains the utility functions to group by and aggregate"

(defn compute-groupby
  "map the result to different files"
  [dataframe num-worker output-dir exception]
  )

(defn compute-aggregate
  "aggregate the output files to the final destination"
  [dateframe output-dir exp])

;; the example of how to write a set of aggregate function
(defn min-pre
  []
  (reset! memo 1))

(defn min
  [row]
  (reset! memo (min (deref memo) row)))

(defn min-result
  []
  (deref memo))