(ns clojask.sort
  "External (larger-than-memory) sort of a CSV file."
  (:require [clojure.java.io :as io])
  (:import [com.google.code.externalsorting.csv CsvExternalSort CsvSortOptions$Builder]
           [java.io File]))

(defn use-external-sort
  "Sort the CSV file at input into output with comp, a comparator over
   parsed rows. Temporary chunks go under .clojask/sort."
  [input output comp]
  ;; clean the output file
  (with-open [wtr (io/writer output)]
    (.write wtr ""))
  (io/make-parents "./.clojask/sort/a.txt")
  (let [input (File. input)
        output (File. output)
        sort-option (let [builder (CsvSortOptions$Builder. comp CsvExternalSort/DEFAULTMAXTEMPFILES (* 5 (CsvExternalSort/estimateAvailableMemory)))]
                      (.numHeader builder 1)
                      (.skipHeader builder false)
                      (.build builder))
        header (java.util.ArrayList.)
        file-list (CsvExternalSort/sortInBatch input (File. "./.clojask/sort") sort-option header)]
    (str "Sorted in total " (CsvExternalSort/mergeSortedFiles file-list output sort-option true header) " rows.")))
