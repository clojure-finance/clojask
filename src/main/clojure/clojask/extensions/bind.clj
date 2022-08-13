(ns clojask.extensions.bind
  "Contains functions that extends the power of clojask, while not directly applying to the dataframe class"
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojask.dataframe :as ck]
            [clojask-io.input :refer [read-file]]
            [clojask-io.output :refer [write-csv]]))

(defn _cbind
  "joins a list of lazy sequences vertically"
  [seq]
  (apply map (fn [a b & cs] (apply concat (concat [a b] cs))) seq))

(defn cbind-csv
  "Joins some csv files into a new dataframe by columns"
  [a b & cs]
  (let [files (concat [a b] cs)
        func (fn [] {:clojask-io true
                     :data (_cbind (map (fn [file] (:data (read-file file :format "csv" :stat true))) files))
                     :size (reduce (fn [a b] (if (and (not= a nil) (not= b nil)) (+ a b) nil)) (mapv (fn [file] (:size (read-file file :format "csv" :stat true))) files))
                     :output (fn [wtr seq] (write-csv wtr seq ","))})]
    (ck/dataframe func)
    ;; (func)
    ))

(defn cbind
  "Joins some dataset files into a new dataframe by columns.\n
   If one of the file does not use the default seperator, please rewrite this function!"
  [a b & cs]
  (let [files (concat [a b] cs)
        func (fn [] {:clojask-io true
                     :data (_cbind (map (fn [file] (:data (read-file file :stat true))) files))
                     :size (reduce (fn [a b] (if (and (not= a nil) (not= b nil)) (+ a b) nil)) (mapv (fn [file] (:size (read-file file :stat true))) files))})]
    (ck/dataframe func)))

(defn rbind-csv
  "Joins some csv files into a new dataframe by rows\n
   Will by default use the header names of the first file"
  [a b & cs]
  (let [files (concat [b] cs)
        files-witha (concat [a b] cs)
        func (fn [] {:clojask-io true
                     :data (concat (:data (read-file a :format "csv")) (apply concat (map rest (map #(:data (read-file % :format "csv")) files))))
                     :size (reduce (fn [a b] (if (and (not= a nil) (not= b nil)) (+ a b) nil)) (mapv (fn [file] (:size (read-file file :format "csv" :stat true))) files-witha))
                     :output (fn [wtr seq] (write-csv wtr seq ","))})]
    (ck/dataframe func)))

(defn rbind
  "Joins some csv files into a new dataframe by rows\n
   Will by default use the header names of the first file"
  [a b & cs]
  (let [files (concat [b] cs)
        files-witha (concat [a b] cs)
        func (fn [] {:clojask-io true
                     :data (concat (:data (read-file a)) (apply concat (map rest (map #(:data (read-file %)) files))))
                     :size (reduce (fn [a b] (if (and (not= a nil) (not= b nil)) (+ a b) nil)) (mapv (fn [file] (:size (read-file file :stat true))) files-witha))})]
    (ck/dataframe func)))