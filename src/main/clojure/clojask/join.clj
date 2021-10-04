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
  [dist a-row a-keys]
  ;; (def output-filename dist)
  ;; (doseq [i (take (count a-keys) (iterate inc 0))]
  ;;   (def output-filename (str output-filename "_" (name (nth b-keys i)) "-" (nth a-row (get a-map (nth a-keys i))))))
  ;; (str output-filename ".csv")
  (let [a-val (mapv (fn [_] (nth a-row _)) a-keys)]
    (str dist a-val)))

(defn output-join
  [writer a-row a-keys a-map b-keys a-format b-format a-index b-index]
  (let [filename (gen-join-filenames "_clojask/join/b/" a-row a-keys)]
    ;; (println writer)
    ;; (spit "_clojask/join/test.txt" (str writer "\n") :append true)
    ;; (.write writer (str [a-row a-keys a-map b-keys a-format b-format a-index b-index] "\n"))
    (if (.exists (io/file filename))
      ;; (.write writer (str (map type a-row) "\n"))
      ;; (spit "_clojask/join/test.txt" (str (vec (read-csv-seq filename)) "\n") :append true)
      (let [a-row (for [index a-index]
                    (if-let [format (get a-format index)]
                      (format (nth a-row index))
                      (nth a-row index)))]
        (doseq [b-row (read-csv-seq filename)]
          ;; (.write writer (str (map type b-row) "\n"))
        ;; (spit "_clojask/join/test.txt" (str a-row b-row "\n") :append true)
          (let [b-row (for [index b-index]
                        (if-let [format (get b-format index)]
                          (format (nth b-row index))
                          (nth b-row index)))]
            (.write writer (str (vec (concat a-row b-row)) "\n"))))))))

(defn output-join-loo
  "used for left join right join or outter join"
  [writer a-row a-keys a-map b-keys count a-format b-format a-index b-index] 
  (let [filename (gen-join-filenames "_clojask/join/b/" a-row a-keys)]
    ;; (println writer)
    ;; (spit "_clojask/join/test.txt" (str writer "\n") :append true)
    (if (.exists (io/file filename))
      ;; (spit "_clojask/join/test.txt" (str (vec (read-csv-seq filename)) "\n") :append true)
      (doseq [b-row (read-csv-seq filename)]
        ;; (spit "_clojask/join/test.txt" (str a-row b-row "\n") :append true)
        (let [a-row (for [index a-index]
                      (if-let [format (get a-format index)]
                        (format (nth a-row index))
                        (nth a-row index)))
              b-row (for [index b-index]
                      (if-let [format (get b-format index)]
                        (format (nth b-row index))
                        (nth b-row index)))]
          (.write writer (str (vec (concat a-row b-row)) "\n"))))
      (let [a-row (for [index a-index]
                    (if-let [format (get a-format index)]
                      (format (nth a-row index))
                      (nth a-row index)))]
        (.write writer (str (vec (concat a-row (replicate count ""))) "\n"))))))

(defn roll-join-get-line-forward
  "get the max of all the smaller"
  [bench filename index]
  (def memo (volatile! nil))
  (def res (volatile! nil))
  (doseq [row (read-csv-seq filename)]
    (let [val (nth row index)]
      (if (and (> (compare val bench) 0) (or (= @memo nil) (< (compare val @memo) 0)))
        (do (vreset! memo val)
            (vreset! res row)))))
  @res)

(defn roll-join-get-line-backward
  "get the max of all the smaller"
  [bench filename index]
  (def memo (volatile! nil))
  (doseq [row (read-csv-seq filename)]
    (let [val (nth row index)]
      (if (and (> (compare val bench) 0) (< (compare val @memo) 0))
        (vreset! memo val))))
  @memo)

(doseq [file (rest (file-seq (clojure.java.io/file "./_clojask/grouped/")))]
  (io/delete-file file))

;; (defn internal-rolling-join-forward
;;   [a b a-dir b-dir a-keys b-keys a-roll b-roll]
;;   ;; (let [a-reader (io/reader (:path a))]
;;   ;;   ())
;;   (s))


(defn output-join-forward
  ""
  [writer a-row a-keys a-map b-keys count a-roll b-roll a-format b-format a-index b-index]
  (let [filename (gen-join-filenames "_clojask/join/b/" a-row a-keys)]
    ;; (println writer)
    ;; (spit "_clojask/join/test.txt" (str writer "\n") :append true)
    (if (.exists (io/file filename))
      ;; (spit "_clojask/join/test.txt" (str (vec (read-csv-seq filename)) "\n") :append true)
      (if-let [b-row (roll-join-get-line-forward (nth a-row a-roll) filename b-roll)] ;; bench is a string
        (let [a-row (for [index a-index]
                      (if-let [format (get a-format index)]
                        (format (nth a-row index))
                        (nth a-row index)))
              b-row (for [index b-index]
                      (if-let [format (get b-format index)]
                        (format (nth b-row index))
                        (nth b-row index)))]
         (.write writer (str (vec (concat a-row b-row)) "\n")))
        (let [a-row (for [index a-index]
                      (if-let [format (get a-format index)]
                        (format (nth a-row index))
                        (nth a-row index)))]
          (.write writer (str (vec (concat a-row (replicate count ""))) "\n")))
        )
      (let [a-row (for [index a-index]
                    (if-let [format (get a-format index)]
                      (format (nth a-row index))
                      (nth a-row index)))]
        (.write writer (str (vec (concat a-row (replicate count ""))) "\n"))))))