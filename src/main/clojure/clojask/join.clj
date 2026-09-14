(ns clojask.join
  (:require [clojure.java.io :as io]
            [clojask.groupby :refer [read-csv-seq]]
            [clojask.utils :as u]))

(def source nil)

(defn gen-join-filenames
  [dist a-row a-keys]
  (let [a-val (mapv (fn [_] ((or (nth _ 0) identity) (nth a-row (nth _ 1)))) a-keys)]
    (if (nil? dist) (str a-val) (str dist (u/encode-str (str a-val))))))

(defn output-join-inner
  [writer a-row a-keys a-map b-keys count a-roll b-roll a-format b-format a-index b-index join-index write-func]
  (let [filename (gen-join-filenames ".clojask/join/b/" a-row a-keys)]
    (if (.exists (io/file filename))
      (let [a-row (u/gets-format a-row a-index a-format)
            filename (io/reader filename)]
        (doseq [b-row (read-csv-seq filename)]
          (let [b-row (u/gets b-row b-index)]
            (write-func writer (vector (u/gets (concat a-row b-row) join-index)))))
        (.close filename)))))

(defn output-join-inner-mem
  [writer a-row a-keys a-map b-keys count a-roll b-roll a-format b-format a-index b-index join-index write-func]
  (let [filename (gen-join-filenames nil a-row a-keys)]
    (if (.exists source filename)
      (let [a-row (u/gets-format a-row a-index a-format)]
        (doseq [b-row (.getKey source filename)]
          (let []
            (write-func writer (vector (u/gets (concat a-row b-row) join-index)))))))))

(defn output-join-loo
  "used for left join right join or outter join"
  [writer a-row a-keys a-map b-keys count a-roll b-roll a-format b-format a-index b-index join-index write-func]
  (let [filename (gen-join-filenames ".clojask/join/b/" a-row a-keys)]
    (if (.exists (io/file filename))
      (let [filename (io/reader filename)]
        (doseq [b-row (read-csv-seq filename)]
          (let [a-row (u/gets-format a-row a-index a-format)
                b-row (u/gets b-row b-index)
                ]
            (write-func writer (vector (u/gets (concat a-row b-row) join-index)))))
        (.close filename))
      (let [a-row (u/gets-format a-row a-index a-format)]
       (write-func writer (vector (u/gets (concat a-row (repeat count "")) join-index)))))))

(defn output-join-loo-mem
  "used for left join right join or outter join"
  [writer a-row a-keys a-map b-keys count a-roll b-roll a-format b-format a-index b-index join-index write-func]
  (let [filename (gen-join-filenames nil a-row a-keys)]
    (if (.exists source filename)
      (let [b-rows (.getKey source filename)]
        (doseq [b-row b-rows]
          (let [a-row (u/gets-format a-row a-index a-format)
                ]
            (write-func writer (vector (u/gets (concat a-row b-row) join-index))))))
      (let [a-row (u/gets-format a-row a-index a-format)]
        (write-func writer (vector (u/gets (concat a-row (repeat count "")) join-index)))))))

(defn defn-join
  [type limit _source]
  (def source _source)
  (def output-join
    (case type
      1 (if (nil? _source) output-join-inner output-join-inner-mem)
      2 (if (nil? _source) output-join-loo output-join-loo-mem)
      ;; 4 output-join-forward
      4 (let [roll-join-get-line-forward (fn [bench filename index]
                                           (def memo (volatile! nil))
                                           (def res (volatile! nil))
                                           (doseq [row (read-csv-seq filename)]
                                             (let [val (nth row index)]
                                               (if (and (<= (compare val bench) 0) (limit bench val) (or (= @memo nil) (> (compare val @memo) 0)))
                                                 (do (vreset! memo val)
                                                     (vreset! res row)))))
                                           @res)
              roll-join-get-line-forward-mem (fn [bench filename index]
                                               (def memo (volatile! nil))
                                               (def res (volatile! nil))
                                               (doseq [row (.getKey source filename)]
                                                 (let [unformat (nth row 1)
                                                       val (nth unformat index)]
                                                   (if (and (<= (compare val bench) 0) (limit bench val) (or (= @memo nil) (> (compare val @memo) 0)))
                                                     (do (vreset! memo val)
                                                         (vreset! res (first row))))))
                                               @res)]
          (if (nil? _source)
            (fn [writer a-row a-keys a-map b-keys count a-roll b-roll a-format b-format a-index b-index join-index write-func]
              (let [filename (gen-join-filenames ".clojask/join/b/" a-row a-keys)]
                (if (.exists (io/file filename))
                  (let [filename (io/reader filename)]
                    (if-let [b-row (roll-join-get-line-forward (nth a-row a-roll) filename b-roll)] ;; bench is a string
                      (let [;; a-row (for [index a-index]
                            a-row (u/gets-format a-row a-index a-format)
                            b-row (u/gets-format b-row b-index b-format)]
                        (write-func writer [(u/gets (concat a-row b-row) join-index)]))
                      (let [a-row (for [index a-index]
                                    (if-let [format (get a-format index)]
                                      (format (nth a-row index))
                                      (nth a-row index)))]
                        (write-func writer [(u/gets (concat a-row (repeat count "")) join-index)])))
                    (.close filename))
                  (let [a-row (u/gets-format a-row a-index a-format)]
                    (write-func writer [(u/gets (concat a-row (repeat count "")) join-index)])))))
            (fn [writer a-row a-keys a-map b-keys count a-roll b-roll a-format b-format a-index b-index join-index write-func]
              (let [filename (gen-join-filenames nil a-row a-keys)]
                (if (.exists source filename)
                  (let []
                    (if-let [b-row (roll-join-get-line-forward-mem (nth a-row a-roll) filename b-roll)] ;; bench is a string
                      (write-func writer [(u/gets (concat (u/gets-format a-row a-index a-format) b-row) join-index)])
                      (write-func writer [(u/gets (concat (u/gets-format a-row a-index a-format) (repeat count "")) join-index)])))
                  (let [a-row (u/gets-format a-row a-index a-format)]
                    (write-func writer [(u/gets (concat a-row (repeat count "")) join-index)])))))))
      ;; 5 output-join-backward
      5 (let [roll-join-get-line-backward (fn [bench filename index]
                                            (def memo (volatile! nil))
                                            (def res (volatile! nil))
                                            (doseq [row (read-csv-seq filename)]
                                              (let [val (nth row index)]
                                                (if (and (>= (compare val bench) 0) (limit bench val) (or (= @memo nil) (< (compare val @memo) 0)))
                                                  (do (vreset! memo val)
                                                      (vreset! res row)))))
                                            @res)
              roll-join-get-line-backward-mem (fn [bench filename index]
                                               (def memo (volatile! nil))
                                               (def res (volatile! nil))
                                               (doseq [row (.getKey source filename)]
                                                 (let [unformat (nth row 1)
                                                       val (nth unformat index)]
                                                   (if (and (>= (compare val bench) 0) (limit bench val) (or (= @memo nil) (< (compare val @memo) 0)))
                                                     (do (vreset! memo val)
                                                         (vreset! res (first row))))))
                                               @res)]
          (if (nil? source)
           (fn
            [writer a-row a-keys a-map b-keys count a-roll b-roll a-format b-format a-index b-index join-index write-func]
            (let [filename (gen-join-filenames ".clojask/join/b/" a-row a-keys)]
              (if (.exists (io/file filename))
                (let [filename (io/reader filename)]
                  (if-let [b-row (roll-join-get-line-backward (nth a-row a-roll) filename b-roll)] ;; bench is a string
                    (let [a-row (u/gets-format a-row a-index a-format)
                          b-row (u/gets-format b-row b-index b-format)]
                      (write-func writer [(u/gets (concat a-row b-row) join-index)]))
                    (let [a-row (u/gets-format a-row a-index a-format)]
                      (write-func writer [(u/gets (concat a-row (repeat count "")) join-index)])))
                  (.close filename))
                (let [a-row (u/gets-format a-row a-index a-format)]
                  (write-func writer [(u/gets (concat a-row (repeat count "")) join-index)])))))
            (fn [writer a-row a-keys a-map b-keys count a-roll b-roll a-format b-format a-index b-index join-index write-func]
              (let [filename (gen-join-filenames nil a-row a-keys)]
                (if (.exists source filename)
                  (let []
                    (if-let [b-row (roll-join-get-line-backward-mem (nth a-row a-roll) filename b-roll)] ;; bench is a string
                      (write-func writer [(u/gets (concat (u/gets-format a-row a-index a-format) b-row) join-index)])
                      (write-func writer [(u/gets (concat (u/gets-format a-row a-index a-format) (repeat count "")) join-index)])))
                  (let [a-row (u/gets-format a-row a-index a-format)]
                    (write-func writer [(u/gets (concat a-row (repeat count "")) join-index)])))))))
      nil)))