(ns clojask.stream-input
  (:require [clojure.core.async :refer [chan >!! <!! close!]]
            [onyx.extensions :as extensions]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api]
            [tech.v3.dataset :as ds])
  (:gen-class))

(def input-segments
  [{:id 0 :map {:seg 1}}
   {:id 1 :map {:seg 2}}
   {:id 2 :map {:seg 3}}
   {:id 3 :map {:seg 4}}
   {:id 4 :map {:seg 5}}])


;; This the the way to synchronously input the data (copied from demo)
(def input-chan (chan capacity))
(def input-buffer (atom {}))
(defn prepare-input
  []
  (doseq [segment input-segments]
    (>!! input-chan segment))
  (close! input-chan))

;; the final outcome I want to see is that
;; the function is given a lazy sequence, and the function uses another thread to inject
;; this seq into a new input channel / a given input channel.
;; the function immediately returns the input channel
;; @ overload
(defn stream-input
  ([data])
  ([data input-cha]))