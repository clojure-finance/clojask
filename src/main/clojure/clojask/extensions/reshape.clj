(ns clojask.extensions.reshape
  "Reshape a clojask dataframe between wide and long layouts."
  (:require [clojask.dataframe :as ck]))

(defn melt
  "Reshape the clojask dataframe from wide to long: every row becomes one
   row per measure column, holding the id columns, the measure's name and
   its value."
  [df output-dir id measure & {:keys [measure-name value-name] :or {measure-name "measure" value-name "value"}}]
  (let [id-count (count id)
        mea-count (count measure)
        func (fn [x] (map concat (repeat (take id-count x)) (map vector measure (take-last mea-count x))))]
    (ck/compute df 1 output-dir :select (concat id measure) :melt func :header (concat id [measure-name value-name]))))

(defn dcast
  "Reshape the clojask dataframe from long to wide: the rows that share the
   id columns collapse into one row with a column per entry of vals, filled
   from value-name where measure-name matches; missing entries are empty.
   The operations are appended to df itself."
  [df output-dir id measure-name value-name vals & {:keys [vals-name] :or {vals-name vals}}]
  (assert (= [] (.getGroupbyKeys (:row-info df))) "dcast is not applicable to a dataframe that is already grouped")
  (assert (= (count vals) (count vals-name)) "vals-name must have one name per entry of vals")
  (ck/operate df vector [measure-name value-name] "dcast1014")
  (ck/group-by df id)
  (doseq [[i v] (map-indexed vector vals)]
    (ck/aggregate df
                  (fn [pairs] (or (some (fn [[m value]] (when (= m v) value)) pairs) ""))
                  "dcast1014"
                  (str "dcast1014_" i)))
  (ck/compute df 8 output-dir :header (concat id vals-name)))
