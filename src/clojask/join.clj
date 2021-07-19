(ns clojask.join
  (:require [clojure.java.io :as io]
            [clojure-csv.core :as csv]
            [clojure.core.async :as async]
            [clojask.onyx-comps :refer [start-onyx-groupby]]
            [clojask.groupby :refer [read-csv-seq]]
            [clojure.string :as string])
  (:import (java.io File)))

(def num-worker 8)
(def batch-size 10)

(defn- group-inner-join
  [a b c]
  ;; a is readers to the file
  ;; b is the filename
  (with-open [wtr (io/writer c :append true)]
    (doseq [a-row (read-csv-seq a)]
      (doseq [b-row (read-csv-seq b)]
        (.write wtr (str (vec (concat a-row b-row)) "\n"))))))

(defn internal-inner-join
  [a b a-keys b-keys]
  ;; create the file structure
  (io/make-parents "./_clojask/join/a/a.txt")
  (io/make-parents "./_clojask/join/b/a.txt")
  ;; first group a and b by keys
  (start-onyx-groupby num-worker batch-size a "./_clojask/join/a/" a-keys false)
  (start-onyx-groupby num-worker batch-size b "./_clojask/join/b/" b-keys false)

  ;; then join a and b by filename
  (let [directory (clojure.java.io/file "./_clojask/join/a/")
        files (file-seq directory)]
    (doseq [file (rest files)]
      (def filename (.toString file))
      (def filename (string/replace-first filename "_clojask/join/a/" "_clojask/join/b/"))
      (doseq [i (take (count a-keys) (iterate inc 0))]
        (def filename (string/replace-first filename (str "_" (nth a-keys i)) (str "_" (nth b-keys i)))))
      (try
        (let [reader (io/reader filename)]
          (group-inner-join file filename (io/writer (string/replace-first (.toString file) "_clojask/join/a/" "_clojask/join/"))))
        ;; delete the files after joining
        (io/delete-file file true)
        (io/delete-file (File filename) true)
        (catch Exception e
          (do
            nil))))))