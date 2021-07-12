(ns clojask.sort
  (:require [clojure.java.io :as io]
            [clojure.data.csv :as csv]
            [clojask.groupby :as gb])
  (:import [com.google.code.externalsorting.csv  CsvExternalSort]
           [com.google.code.externalsorting.csv  CsvSortOptions CsvSortOptions$Builder]
           [java.io File]))

(defn template-compare?
  ;; row1 is the first row
  ;; row2 is the second row

  ;;return is a int (- / 0 / +)
  [row1 row2]
  )

(defn salary-compare?
  [row1 row2]
  (- (Integer/parseInt (get row1 :Salary)) (Integer/parseInt (get row2 :Salary))))

(defn prc-compare?
  [row1 row2]
;;     (println row1)
;;     (println row2)
  (if (= (get row1 :PRC) "")
    -1
    (if (= (get row2 :PRC) "")
      +1
      (- (Double/parseDouble (get row1 :PRC)) (Double/parseDouble (get row2 :PRC))))))


(defn get-seq
  [input-dir]
  (let [csv-data (csv/read-csv (io/reader input-dir))]
;;         (println (first csv-data))
    (map zipmap ;; make the first row as headers and the following rows as values in a map structure e.g. {:tic AAPL} 
         (->> (first csv-data) ;; take the first row of the csv-data
              (map keyword) ;; make the header be the "key" in the map 
              repeat)      ;; repeat the process for all the headers
         (rest csv-data))))

(defn internal-sort-large
  [input-dir out-dir comparator]
  (def curr (atom nil))
  (def prev (atom nil))
  (def has-next? (atom true))
  (with-open [wtr (io/writer out-dir :append true)]
    (loop []
      (reset! curr nil)
          ;; the first iteration is to find the standard
      (doseq [row (get-seq input-dir)]
        (if (and (or (= (deref prev) nil) (> (comparator row (deref prev)) 0)) (or (= (deref curr) nil) (< (comparator row (deref curr)) 0)))
          (do
            (reset! curr row)
            (reset! has-next? true))))
;;         (println (deref curr))
      (if (deref has-next?)
        (do
          (doseq [row (get-seq input-dir)]
            (if (= (compare 0 (comparator row (deref curr))) 0)
              (.write wtr (str row "\n"))))
          (reset! prev (deref curr))
          (reset! has-next? false)
          (.flush wtr)
          (recur))
        nil))
    "success"))


(defn internal-sort-small
  [input-dir out-dir comparator]
  (with-open [wtr (io/writer out-dir)]
    (doseq [row (sort prc-compare? (get-seq input-dir))]
      (.wtr (str row "\n"))))
  "success"
  )

(defn comp
  [a b]
  (compare (.get a 0) (.get b 0)))

(defn use-external-sort
  [input output comp]
  (let 
   [input (File. input)
    output (File. output)
    sort-option (let [builder (CsvSortOptions$Builder. comp CsvExternalSort/DEFAULTMAXTEMPFILES (CsvExternalSort/estimateAvailableMemory))]
                  (.numHeader builder 1)
                  (.skipHeader builder false)
                  (.build builder))
    ;; header (vec (first (csv/read-csv (io/reader input))))
    header (java.util.ArrayList.)
    file-list (CsvExternalSort/sortInBatch input nil sort-option header)
    ]
    ;; (println sort-option)
    (println header)
    (str "Sorted in total "(CsvExternalSort/mergeSortedFiles file-list output sort-option true header) " rows.")))
