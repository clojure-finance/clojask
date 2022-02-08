(ns clojask.extensions
  "Contains functions that extends the power of clojask, while not directly applying to the dataframe class"
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojask.dataframe :refer [dataframe]]
            [clojure.string :as string]))



(defn _cbind
  "joins a list of lazy sequences vertically"
  [seq]
  (apply map (fn [a b & cs] (string/join "," (concat [a b] cs))) seq))

(defn cbind-csv
  "joins some csv files into a new dataframe by columns"
  [a b & cs]
  (let [files (concat [a b] cs)
        func (fn [] (_cbind (map #(line-seq (io/reader %)) files)))]
    (dataframe func)))

(defn rbind-csv
  "Joins some csv files into a new dataframe by rows\n
   Will by default use the header names of the first file"
  [a b & cs]
  (let [files (concat [a b] cs)
        seq (map #(line-seq (io/reader %)) files)
        func (fn [] (conj (apply concat (map rest seq)) (first (first seq))))]
    (dataframe func)))

