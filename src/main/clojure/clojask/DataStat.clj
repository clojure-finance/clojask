(ns clojask.DataStat
  (:require [clojure.java.io :as io]))

(import '[com.clojask.exception TypeException]
        '[com.clojask.exception OperationException])

(definterface DataIntf
  (init [source file])
  (initWithIO [io-func])
  (getSize []))


(deftype DataStat
  ;; the column description about whether a change is made to this column
         [^:unsynchronized-mutable file-size
          ^:unsynchronized-mutable num-rows]

  ;; method
  DataIntf

  (init
   [this source file]
   (if file
     (do
       (set! file-size (:size (file)))
       (set! num-rows nil))
     (if (fn? source)
       (do
         (set! file-size nil)
         (set! num-rows nil))
       (do
         (set! file-size (.length (io/file source)))))))
  
  (initWithIO
   [this io-func]
   (set! file-size (:size (io-func)))
   (set! num-rows nil))

   (getSize
    [this]
    file-size))

(defn compute-stat
  [source & [io-func]]
  (let [stat (DataStat. nil nil)]
    (if io-func
      (.initWithIO stat io-func)
      (.init stat source nil))
    stat))