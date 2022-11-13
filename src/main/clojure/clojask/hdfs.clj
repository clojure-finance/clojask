(ns clojask.hdfs
  (:require [clojure.java.io :as io])
  (:import [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs FSDataInputStream FileSystem Path]
           [java.net URI]) 
  (:require [clojure.java.io :as io]))

(def conf  (Configuration.))
(.set conf "fs.hdfs.impl" "org.apache.hadoop.hdfs.DistributedFileSystem")

(def fs nil)

(def path-prefix "/clojask/")

(defn path
  [path]
  (str path-prefix path))

(defn init
  []
  (def fs (FileSystem/get (URI. "hdfs://localhost:9000") conf))
  (assert (.mkdirs fs (Path. path-prefix)) "HDFS init fails.")
  )

(defn write
  [path-str msg]
  (with-open [wtr (.append fs (path path-str))
              buff (.getBytes msg)]
     (.write wtr buff 0 (.length buff))))

(defn read
  [path-str]
  (with-open [rdr (.open (path path-str))
              rdr (io/reader rdr)]
    (.read rdr)
    )
  )

(defn close
  []
  (.close fs))

