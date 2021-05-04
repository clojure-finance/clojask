(ns clojask.delayed)

(deftype delayed
  [^map element-operation
   ^map row-operation
   ^map advanced-operation]
  (functions [] ))