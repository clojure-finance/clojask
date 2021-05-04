(ns clojask.dataframe)
"The clojask lazy dataframe"

;; each dataframe can have a delayed object
(defrecord dataframe
           [delayed]
  boolean
  (compute
    [this]))