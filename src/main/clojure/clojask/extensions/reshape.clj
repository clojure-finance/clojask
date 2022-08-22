(ns clojask.extensions.reshape
  "Contains functions that extends the power of clojask, while not directly applying to the dataframe class"
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojask.dataframe :as ck]))

(defn melt
  "Reshape the clojask dataframe from wide to long."
  [df output-dir id measure & {:keys [measure-name value-name] :or {measure-name "measure" value-name "value"}}]
  (let [id-count (count id)
        mea-count (count measure)
        func (fn [x] (map concat (repeat (take id-count x)) (map vector measure (take-last mea-count x))))]
    (ck/compute df 1 output-dir :select (concat id measure) :melt func :header (concat id [measure-name value-name])))
  )

(defn- dcast-second
  [seq]
  (first (rest seq)))

(defn- dcast-1
  [seq order]
  (let [keys (map first seq)
        vals (map dcast-second seq)
        dict (zipmap keys vals)
        func (fn [order] (if-let [res (get dict order)] (str res) ""))]
    (str/join "," (mapv func order))))

(defn dcast
  "Reshape the clojask dataframe from long to wide."
  [x output-dir id measure-name value-name vals & {:keys [vals-name] :or {vals-name vals}}]
  (assert (= [] (.getGroupbyKeys x)) "dcast is not applicable to this dataframe")
  (ck/operate x (fn [a b] [a b]) [measure-name value-name] "dcast1014")
  (ck/group-by x id)
  (let [func #(dcast-1 % vals)]
    (ck/aggregate x func "dcast1014"))
  (ck/compute x 8 output-dir :header (concat id vals-name))
  )