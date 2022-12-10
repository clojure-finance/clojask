(ns clojask.classes.MGroup
  (:require [clojure.set :as set]
            [clojask.utils :refer []])
  (:import [com.clojask.exception ExecutionException]))

(definterface MGroupIntf
  (reset [])
  (final [])
  (getKeys [])
  (write [key row] "mimic a bufferedwriter, add a row to a group")
  (getKey [key]))

(deftype MGroup
         [^:unsynchronized-mutable groups]
  
  MGroupIntf
  (reset
   [this]
   (set! groups (transient {})))
  
  (final
   [this]
   (set! groups (persistent! groups)))
  
  (getKeys
   [this]
   (keys groups))
  
  (write
   [this key row]
   (if-let [group (get groups key)]
     (set! groups (assoc! groups key (conj! group row)))
     (set! groups (assoc! groups key (transient [row])))))
  
  (getKey
   [this key]
   (persistent! (get groups key))))
