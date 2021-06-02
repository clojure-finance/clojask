(ns clojask.DataFrame
  (:require [clojask.ColInfo :refer :all]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojask.utils :refer [eval-res]]
            [clojask.onyx-comps :refer [start-onyx]])
  (:import [clojask.ColInfo ColInfo]
           [java.io BufferedReader FileReader BufferedWriter FileWriter]))
"The clojask lazy dataframe"

(definterface DFIntf
  ;; (compute [& {:keys [num-worker output-dir _ _2] :or {num-worker 1 output-dir "resources/test.csv" _ nil _2 nil}}])
  (compute [output-dir])
  (operate [operation colName])
  (colDesc [])
  (head [n])

  (assoc [dataframe])  ;; combine another dataframe with the current
  )

;; each dataframe can have a delayed object
(defrecord DataFrame
           [^String path
            ^Integer batch-size
   ;;  ...
   ;; more fields to add
   ;;  ...
            ^ColInfo col-info]
  DFIntf
  (operate
   [this operation colName]
   (.operate col-info operation colName))
  (colDesc
   [this]
   (.getMap col-info))
  (head
   [this n]
   ())
    ;; currently put read file here
  ^String
  (compute
  ;;  [this & {:keys [num-worker output-dir _ _2] :or {num-worker 1 output-dir "resources/test.csv" _ nil _2 nil}}]
   [this output-dir]
   (if (<= 1 1)
     (with-open [rdr (io/reader path) wtr (io/writer  output-dir)]
      ;; (with-open [rdr (io/reader path) wtr (io/output-stream "test.txt")]
       (.write wtr (str (clojure.string/join "," (map name (.getKeys col-info))) "\n"))
       (doseq [line (rest (csv/read-csv rdr))]
         (let [keys (.getKeys col-info)
               row (zipmap keys line)]
           (doseq [key keys]
             (.write wtr (str (eval-res row (key (.getMap col-info)))))
             (.write wtr ","))
           (.write wtr "\n")))
       (.flush wtr)
       "success")
     (try
       (let [res (start-onyx 1 batch-size this output-dir)]
         (if (= res "success")
           "success"
           "failed"))
       (catch Exception e (str "failed: " (.getMessage e)))))))

(defn dataframe
  [path]
  (try
    (let [reader (BufferedReader. (FileReader. path))
          colNames (doall (first (csv/read-csv reader)))
          ;; colNames ["test"]
          col-info (ColInfo. (doall (map keyword colNames)) {})]
      (.init col-info colNames)
      (DataFrame. path 10 col-info))
    (catch Exception e
      (do
        (println "No such file or directory")
        nil))))