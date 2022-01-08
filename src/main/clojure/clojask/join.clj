(ns clojask.join
  (:require [clojure.java.io :as io]
            [clojure.core.async :as async]
            ;; [clojask.onyx-comps :refer [start-onyx-groupby start-onyx-join]]
            [clojask.groupby :refer [read-csv-seq gen-groupby-filenames]]
            [clojure.string :as str]
            [clojask.utils :as u]))



(defn gen-join-filenames
  [dist a-row a-keys]
  ;; (def output-filename dist)
  ;; (doseq [i (take (count a-keys) (iterate inc 0))]
  ;;   (def output-filename (str output-filename "_" (name (nth b-keys i)) "-" (nth a-row (get a-map (nth a-keys i))))))
  ;; (str output-filename ".csv")
  (let [a-val (mapv (fn [_] ((or (nth _ 0) identity) (nth a-row (nth _ 1)))) a-keys)]
    (str dist a-val)))

(defn output-join-inner
  [writer a-row a-keys a-map b-keys count a-roll b-roll a-format b-format a-index b-index join-index]
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
                      (nth a-row index)))
            filename (io/reader filename)]
        (doseq [b-row (read-csv-seq filename)]
          ;; (.write writer (str (map type b-row) "\n"))
        ;; (spit "_clojask/join/test.txt" (str a-row b-row "\n") :append true)
          (let [b-row (for [index b-index]
                        (if-let [format (get b-format index)]
                          (format (nth b-row index))
                          (nth b-row index)))]
            ;; (println [(vec a-row) (vec b-row) a-index b-index join-index])
            (.write writer (str (str/join "," (vec (u/gets (concat a-row b-row) join-index))) "\n"))))
        (.close filename)))))

(defn output-join-loo
  "used for left join right join or outter join"
  [writer a-row a-keys a-map b-keys count a-roll b-roll a-format b-format a-index b-index join-index]
  (let [filename (gen-join-filenames "_clojask/join/b/" a-row a-keys)]
    ;; (println writer)
    ;; (spit "_clojask/join/test.txt" (str writer "\n") :append true)
    (if (.exists (io/file filename))
      ;; (spit "_clojask/join/test.txt" (str (vec (read-csv-seq filename)) "\n") :append true)
      (let [filename (io/reader filename)]
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
            (.write writer (str (str/join "," (vec (u/gets (concat a-row b-row) join-index))) "\n"))))
        (.close filename))
      (let [a-row (for [index a-index]
                    (if-let [format (get a-format index)]
                      (format (nth a-row index))
                      (nth a-row index)))]
        (.write writer (str (str/join "," (vec (u/gets (concat a-row (repeat count "")) join-index))) "\n"))))))

(defn defn-join
  [type limit]
  (def output-join
    (case type
      1 output-join-inner
      2 output-join-loo
      ;; 4 output-join-forward
      4 (let [roll-join-get-line-forward (fn [bench filename index]
                                           (def memo (volatile! nil))
                                           (def res (volatile! nil))
                                           (doseq [row (read-csv-seq filename)]
                                             (let [val (nth row index)]
                                               (if (and (<= (compare val bench) 0) (limit bench val) (or (= @memo nil) (> (compare val @memo) 0)))
                                                 (do (vreset! memo val)
                                                     (vreset! res row)))))
                                           @res)]
          (fn [writer a-row a-keys a-map b-keys count a-roll b-roll a-format b-format a-index b-index join-index]
            (let [filename (gen-join-filenames "_clojask/join/b/" a-row a-keys)]
              (if (.exists (io/file filename))
                (let [filename (io/reader filename)]
                  (if-let [b-row (roll-join-get-line-forward (nth a-row a-roll) filename b-roll)] ;; bench is a string
                    (let [a-row (for [index a-index]
                                  (if-let [format (get a-format index)]
                                    (format (nth a-row index))
                                    (nth a-row index)))
                          b-row (for [index b-index]
                                  (if-let [format (get b-format index)]
                                    (format (nth b-row index))
                                    (nth b-row index)))]
                      (.write writer (str (str/join "," (vec (u/gets (concat a-row b-row) join-index))) "\n")))
                    (let [a-row (for [index a-index]
                                  (if-let [format (get a-format index)]
                                    (format (nth a-row index))
                                    (nth a-row index)))]
                      (.write writer (str (str/join "," (vec (u/gets (concat a-row (repeat count "")) join-index))) "\n"))))
                  (.close filename))
                (let [a-row (for [index a-index]
                              (if-let [format (get a-format index)]
                                (format (nth a-row index))
                                (nth a-row index)))]
                  (.write writer (str (str/join "," (vec (u/gets (concat a-row (repeat count "")) join-index))) "\n")))))))
      ;; 5 output-join-backward
      5 (let [roll-join-get-line-backward (fn [bench filename index]
                                            (def memo (volatile! nil))
                                            (def res (volatile! nil))
                                            (doseq [row (read-csv-seq filename)]
                                              (let [val (nth row index)]
      ;;        | does here need to be =?
                                                (if (and (>= (compare val bench) 0) (or (= @memo nil) (< (compare val @memo) 0)))
                                                  (do (vreset! memo val)
                                                      (vreset! res row)))))
                                            @res)]
          (fn
            [writer a-row a-keys a-map b-keys count a-roll b-roll a-format b-format a-index b-index join-index]
            (let [filename (gen-join-filenames "_clojask/join/b/" a-row a-keys)]
    ;; (println writer)
    ;; (spit "_clojask/join/test.txt" (str writer "\n") :append true)
              (if (.exists (io/file filename))
      ;; (spit "_clojask/join/test.txt" (str (vec (read-csv-seq filename)) "\n") :append true)
                (let [filename (io/reader filename)]
                  (if-let [b-row (roll-join-get-line-backward (nth a-row a-roll) filename b-roll)] ;; bench is a string
                    (let [a-row (for [index a-index]
                                  (if-let [format (get a-format index)]
                                    (format (nth a-row index))
                                    (nth a-row index)))
                          b-row (for [index b-index]
                                  (if-let [format (get b-format index)]
                                    (format (nth b-row index))
                                    (nth b-row index)))]
                      (.write writer (str (str/join "," (vec (u/gets (concat a-row b-row) join-index))) "\n")))
                    (let [a-row (for [index a-index]
                                  (if-let [format (get a-format index)]
                                    (format (nth a-row index))
                                    (nth a-row index)))]
                      (.write writer (str (str/join "," (vec (u/gets (concat a-row (repeat count "")) join-index))) "\n"))))
                  (.close filename))
                (let [a-row (for [index a-index]
                              (if-let [format (get a-format index)]
                                (format (nth a-row index))
                                (nth a-row index)))]
                  (.write writer (str (str/join "," (vec (u/gets (concat a-row (repeat count "")) join-index))) "\n")))))))
      nil)))