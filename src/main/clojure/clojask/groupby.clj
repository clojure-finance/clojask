(ns clojask.groupby
  (:require [clojure.java.io :as io]
            ;[clojure-csv.core :as csv]
            [clojask.utils :as u]
            [clojask.classes.MGroup :refer [->MGroup]])
  (:import [clojask.classes.MGroup MGroup]))
"contains the utility functions to group by and aggregate"

;; ;; the example of how to write a set of aggregate function

(defn gen-groupby-filenames
  "internal function to generate files csv line with groupby key(s)"
  [dist msg groupby-keys key-index formatters]
  (let [index groupby-keys
        ;; (map (fn [_] (get key-index _)) groupby-keys)
        val (mapv (fn [_] 
                    (let [func (nth _ 0)
                          _ (nth _ 1)]
                     (if func
                       (func (nth msg _))
                       (nth msg _)
                       ))) 
                  index)]
    (if (string? dist) (str dist (u/encode-str (str val))) (str val))))

(defn output-groupby
  "internal function called by output when aggregation is applied"
  [dist msg groupby-keys key-index formatter write-index _format]
  ;; msg this time is a vector

  ;; key-index contains the one to one correspondence of key value to index value, it is a map
  ;; eg "Salary" -> 3
  ;; (spit "resources/debug.txt" (str msg "\n" key-index) :append true)
  (let [output-filename (gen-groupby-filenames dist msg groupby-keys key-index formatter) ;; generate output filename
        ]
    (if (string? dist)
      (with-open [groupby-wrtr (io/writer output-filename :append true)]
        (.write groupby-wrtr (str (if (= true _format) (u/gets-format msg write-index formatter) (u/gets msg write-index)) "\n"))
        (.close groupby-wrtr))
      (.write dist output-filename msg write-index formatter))
    ;; write as maps e.g. {:name "Tim", :salary 62, :tax 0.1, :bonus 12}
    ;; (.write groupby-wrtr (str (u/gets-format msg write-index formatter) "\n"))

    ;; write as csv format e.g. Tim,62,0.1,12
    ;(.write groupby-wrtr (str (clojure.string/join "," (map msg (keys msg))) "\n"))

    ;; close writer
)

  ;; !! debugging
  )

(defn read-csv-seq
  "takes file name and reads data"
  [filename]
    (let [file (io/reader filename)]
      (->> file
           (line-seq)
           (map read-string))))

;; ;; below are example aggregate functions

;; (defn square [n] (* n n))

;; (defn mean [a] (/ (reduce + a) (count a)))

;; ;; !! check if new-keys are float/int cols

;; ;; !! check if new-keys are float/int cols

;; ;; !! check if new-keys are float/int cols

