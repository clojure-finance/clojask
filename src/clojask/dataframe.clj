(ns clojask.DataFrame
  (:require [clojask.ColInfo :refer :all]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io])
  (:import [clojask.ColInfo ColInfo]
           [java.io BufferedReader FileReader BufferedWriter FileWriter]))
"The clojask lazy dataframe"

(definterface DFIntf
  (compute [])
  (compute [output-dir])
  (operate [operation colName])
  (colDesp [])
  (head [n])
  (assoc [dataframe])  ;; combine another dataframe with the current
  )

(def row [])

;; each dataframe can have a delayed object
(defrecord DataFrame
           [^String path
            ^Integer num-instance
   ;;  ...
   ;; more fields to add
   ;;  ...
            ^BufferedReader reader
            ^ColInfo col-info]
  DFIntf
  ^DateFrame
  (operate
   [this operation colName]
   (.operate col-info operation colName))
  (colDesp
    [this]
    (.getMap col-info))
  (head
   [this n]
   ())
    ;; currently put read file here
  (compute
    [this output-dir]
    (if (= num-instance 1)
      (with-open [rdr (BufferedReader. (FileReader. path)) wtr (io/writer  output-dir)]
        (.write wtr (str (clojure.string/join "," (map name (.getKeys col-info))) "\n"))
        (doseq [line (rest (csv/read-csv rdr))]
          (let [row (zipmap (.getKeys col-info) line)]
            (def row row)
;;                    (println row)
;;                    (println (map (fn [_] (_ (.colDesp this))) (.getKeys col-info)))
;;                    (println (eval (read-string (:Salary (.colDesp this)))))
;;                    (println (first (.getKeys col-info)))
            (.write wtr (str (clojure.string/join "," (map (fn [_] (eval (read-string (_ (.colDesp this))))) (.getKeys col-info))) "\n"))
            (println (zipmap (.getKeys col-info) (doall (map (fn [_] (eval (read-string (_ (.colDesp this))))) (.getKeys col-info)))))))
        (.flush wtr))
      
      ())))

(defn dataframe
  [path]
  (try
    (let [reader (BufferedReader. (FileReader. path))
          colNames (first (csv/read-csv reader))
          col-info (ColInfo. (doall (map keyword colNames)) {})]
      (.init col-info colNames)
      (DataFrame. path 1 reader col-info))
    (catch Exception e
      (do
        (println "No such file or directory")
        nil))))