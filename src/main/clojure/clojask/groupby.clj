(ns clojask.groupby
  "Utility functions to group by and aggregate."
  (:require [clojure.java.io :as io]
            [clojask.utils :as u]
            [clojask.classes.MGroup]))

(defn gen-groupby-filenames
  "The group key of a row: a file path under dist when dist is a directory
   path, otherwise the printed key vector."
  [dist msg groupby-keys key-index formatters]
  (let [val (mapv (fn [[func i]] (if func (func (nth msg i)) (nth msg i)))
                  groupby-keys)]
    (if (string? dist) (str dist (u/encode-str (str val))) (str val))))

(defn output-groupby
  "Write one row to its group: appended to the group's file when dist is a
   directory path, otherwise added to dist, an in-memory MGroup."
  [dist msg groupby-keys key-index formatter write-index _format]
  (let [output-filename (gen-groupby-filenames dist msg groupby-keys key-index formatter)]
    (if (string? dist)
      (with-open [groupby-wrtr (io/writer output-filename :append true)]
        (.write groupby-wrtr (str (if (= true _format) (u/gets-format msg write-index formatter) (u/gets msg write-index)) "\n")))
      (.write dist output-filename msg write-index formatter))))

(defn read-csv-seq
  "takes file name and reads data"
  [filename]
  (let [file (io/reader filename)]
    (->> file
         (line-seq)
         (map read-string))))
