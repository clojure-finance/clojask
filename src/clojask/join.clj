(ns clojask.join
  (:require [clojure.java.io :as io]
            [clojure-csv.core :as csv]
            [clojure.core.async :as async]
            ;; [clojask.onyx-comps :refer [start-onyx-groupby start-onyx-join]]
            [clojask.groupby :refer [read-csv-seq gen-groupby-filenames]]
            [clojure.string :as string]))


(defn- group-inner-join
  [a b c]
  ;; a is readers to the file
  ;; b is the filename
  (with-open [wtr (io/writer c :append true)]
    (doseq [a-row (read-csv-seq a)]
      (doseq [b-row (read-csv-seq b)]
        (.write wtr (str (vec (concat a-row b-row)) "\n"))))))

(defn gen-join-filenames
  [dist a-row a-keys a-map b-keys]
  (def output-filename dist)
  (doseq [i (take (count a-keys) (iterate inc 0))]
    (def output-filename (str output-filename "_" (name (nth b-keys i)) "-" (nth a-row (get a-map (nth a-keys i))))))
  (str output-filename ".csv"))

(defn output-join
  [writer a-row a-keys a-map b-keys]
  (let [filename (gen-join-filenames "_clojask/join/b/" a-row a-keys a-map b-keys)]
    ;; (println writer)
    ;; (spit "_clojask/join/test.txt" (str writer "\n") :append true)
    (if (.exists (io/file filename))
      ;; (spit "_clojask/join/test.txt" (str (vec (read-csv-seq filename)) "\n") :append true)
      (doseq [b-row (read-csv-seq filename)]
        ;; (spit "_clojask/join/test.txt" (str a-row b-row "\n") :append true)
        (.write writer (str (vec (concat a-row b-row)) "\n"))))))

(defn output-join-loo
  [writer a-row a-keys a-map b-keys count] 
  (let [filename (gen-join-filenames "_clojask/join/b/" a-row a-keys a-map b-keys)]
    ;; (println writer)
    ;; (spit "_clojask/join/test.txt" (str writer "\n") :append true)
    (if (.exists (io/file filename))
      ;; (spit "_clojask/join/test.txt" (str (vec (read-csv-seq filename)) "\n") :append true)
      (doseq [b-row (read-csv-seq filename)]
        ;; (spit "_clojask/join/test.txt" (str a-row b-row "\n") :append true)
        (.write writer (str (vec (concat a-row b-row)) "\n")))
      (.write writer (str (vec (concat a-row (replicate count ""))) "\n")))))