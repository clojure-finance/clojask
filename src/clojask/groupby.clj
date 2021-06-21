(ns clojask.groupby
  (:require [clojure.java.io :as io]
            [clojure-csv.core :as csv])
  )
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
  (def memo (atom 1)))

(defn min
  [row]
  (reset! memo (min (deref memo) row)))

(defn min-result
  []
  (deref memo))

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

(defn take-csv
  "takes file name and reads data"
  [filename]
  (with-open [file (io/reader filename)]
    (-> file
        (slurp)
        (csv/parse-csv))))

(defn readin-groupby
  "internal function to read in grouped csv files"
  [groupby-keys]
  (def directory (clojure.java.io/file "./_grouped/"))
  (def files (file-seq directory))
  (doseq [file (rest files)]
    ;(println (take-csv file)) ;; debugging
    (take-csv file)))

