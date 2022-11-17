(ns clojask.hdfs
  (:require [clojure.java.io :as io])
  (:import [org.apache.hadoop.conf Configuration]
           [org.apache.hadoop.fs FSDataInputStream FileSystem Path]
           [java.net URI]))

(def conf (Configuration.))
(.set conf "fs.hdfs.impl" "org.apache.hadoop.hdfs.DistributedFileSystem")
(.setBoolean conf "dfs.client.use.datanode.hostname", true)
(.setBoolean conf "dfs.datanode.use.datanode.hostname", true)
(.set conf "fs.defaultFS", "hdfs://namenode:9000")

(def fs nil)

(def path-prefix "/clojask/")

(defn path
  [path]
  (Path. (str path-prefix path)))

(defn init
  []
  (def fs (FileSystem/get (URI. "hdfs://namenode:9000") conf))
  (assert (and (.mkdirs fs (path "")) (.mkdirs fs (path ".clojask/"))) "HDFS init fails.")
  )

(defn write
  [path-str msg]
  (let [path (path path-str)
        buff (.getBytes msg)]
   (let [wtr (if (.exists fs path) (.append fs path) (.create fs path))]
     (.write wtr buff 0 (count buff))
     (.close wtr)
     )))

(defn copy
  "copy files from path1 to path2 on the hdfs"
  [path1 path2]
  (.copyFromLocalFile fs (Path. path1) (path path2))
  )

(defn copy-all
  []
  (copy ".clojask/grouped/" ".clojask/")
  ;; (copy ".clojask/join/" ".clojask/")
  )

(defn read
  [path-str]
  (with-open [rdr (.open fs (path path-str))
              rdr (io/reader rdr)]
    (.readLine rdr)
    )
  )

(defn close
  []
  (.close fs))

