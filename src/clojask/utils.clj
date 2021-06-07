(ns clojask.utils
  (:require [clojure.core.async :refer [chan sliding-buffer >!! close!]]
            [clojure.java.io :refer [resource]]
            [onyx.plugin.core-async :refer [take-segments!]])
  (:import (java.util Date)))
"Utility function used in dataframe"

(defn func-name
  [tmp]
  (clojure.string/replace (clojure.string/replace tmp #"\$" "/") #"\@.+" ""))

(defn wrap-res
  [opr tmp-res]
  `(~opr ~tmp-res))

(defn lazy 
  [wrapped]
  (lazy-seq
   (if-let [s (seq wrapped)]
     (cons (first s) (lazy (rest s))))))

(defn eval-res
  [row opr-vec]
  (loop [res row oprs opr-vec]
      (let [opr (first oprs)
            rest (rest oprs)]
        (if (= (count oprs) 1)
          (opr res)
          (recur (opr res) rest)))))
;;;; Lifecycles utils ;;;;

(def default-channel-size 1000)

(def output-channel-capacity (inc default-channel-size))

(defonce channels (atom {}))
(defonce buffers (atom {}))

(defn get-channel
  ([id] (get-channel id default-channel-size))
  ([id size]
    (if-let [id (get @channels id)]
      id
      (do (swap! channels assoc id (chan (or size default-channel-size)))
          (get-channel id)))))

(defn get-buffer
  [id]
    (if-let [id (get @buffers id)]
      id
      (do (swap! buffers assoc id (atom {}))
          (get-buffer id))))

(defn get-input-channel [id]
  (get-channel id default-channel-size))

(defn inject-in-ch-debug
  [_ lifecycle]
  {:core.async/buffer (get-buffer (:core.async/id lifecycle))
    :core.async/chan (get-channel (:core.async/id lifecycle)
                                  (or (:core.async/size lifecycle)
                                      default-channel-size))})

(defn inject-out-ch-debug
  [_ lifecycle]
  {:core.async/chan (get-channel (:core.async/id lifecycle)
                                  (or (:core.async/size lifecycle)
                                      default-channel-size))})

(def in-calls-debug
  {:lifecycle/before-task-start inject-in-ch-debug})

(def out-calls-debug
  {:lifecycle/before-task-start inject-out-ch-debug})

(defn channel-id-for [lifecycles task-name]
  (->> lifecycles
        (filter #(= task-name (:lifecycle/task %)))
        (map :core.async/id)
        (remove nil?)
        (first)))

(defn bind-inputs! [lifecycles mapping]
  (doseq [[task segments] mapping]
    (let [in-ch (get-input-channel (channel-id-for lifecycles task))]
      (doseq [segment segments]
        (>!! in-ch segment))
      (close! in-ch))))

(defn toInt
  [string]
  (Integer/parseInt string))

(defn toDouble
  [string]
  (Double/parseDouble string))

(defn toString
  [string]
  string)

(defn toDate
  [string]
  (Date. string))

(def operation-type-map
  {toInt "int"
   toDouble "double"
   toString "string"
   toDate "date"})
