(ns clojask.DataFrame
  (:require [clojask.ColInfo :refer [->ColInfo]]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojask.utils :refer :all]
            [clojask.onyx-comps :refer [start-onyx]]
            )
  (:import [clojask.ColInfo ColInfo]))
"The clojask lazy dataframe"

(definterface DFIntf
  ;; (compute [& {:keys [num-worker output-dir] :or {num-worker 1 output-dir "resources/test.csv"}}])
  (compute [^int num-worker ^String output-dir ^boolean exception])
  (operate [operation colName] "operate an operation to column")
  (setType [type colName] "types supported: int double string date")
  (colDesc [])
  (colTypes [])
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
   (.operate col-info operation colName)
   "success")
  (colDesc
   [this]
   (.getDesc col-info))
  (colTypes
   [this]
   (.getType col-info))
  (head
   [this n]
   ())
  (setType
   [this type colName]
   (case type
     "int" (.setType col-info toInt colName)
     "double" (.setType col-info toDouble colName)
     "string" (.setType col-info toString colName)
     "date" (.setType col-info toDate colName)
     "No such type. You could instead write your parsing function as the first operation to this column."))
    ;; currently put read file here
  (compute
  ;;  [this & {:keys [num-worker output-dir] :or {num-worker 1 output-dir "resources/test.csv"}}]
   [this ^int num-worker ^String output-dir ^boolean exception]
  ;;  "success"))
   (if (<= num-worker 1)
     (try
       (with-open [rdr (io/reader path) wtr (io/writer  output-dir)]
      ;; (with-open [rdr (io/reader path) wtr (io/output-stream "test.txt")]
         (.write wtr (str (clojure.string/join "," (map name (.getKeys col-info))) "\n"))
         (if exception
           (doseq [line (rest (csv/read-csv rdr))]
             (let [keys (.getKeys col-info)
                   row (zipmap keys line)]
               (doseq [key keys]
                 (.write wtr (str (eval-res row (key (.getDesc col-info)))))
                 (if (not= key (last keys))(.write wtr ",")))
               (.write wtr "\n")))
           (doseq [line (rest (csv/read-csv rdr))]
             (let [keys (.getKeys col-info)
                   row (zipmap keys line)]
               (doseq [key keys]
                 (try
                   (.write wtr (str (eval-res row (key (.getDesc col-info)))))
                   (catch Exception e nil))
                 (.write wtr ","))
               (.write wtr "\n"))))
         (.flush wtr)
         "success")
       (catch Exception e (str "failed: " (.getMessage e))))
     (try
       (let [res (start-onyx num-worker batch-size this output-dir)]
         (if (= res "success")
           "success"
           "failed"))
       (catch Exception e e)))))

(defn dataframe
  [path]
  (try
    (let [reader (io/reader path)
          colNames (doall (first (csv/read-csv reader)))
          ;; colNames ["test"]
          col-info (ColInfo. (doall (map keyword colNames)) {} {})]
      (.close reader)
      (.init col-info colNames)
      (DataFrame. path 10 col-info))
    (catch Exception e
      (do
        (println "No such file or directory")
        nil))))

(defn compute 
  [this num-worker output-dir & {:keys [exception] :or {exception false}}]
  ;;  "success"))
   (.compute this num-worker output-dir exception))