(ns clojask.classes.MGroup
  (:require [clojure.set :as set]
            [clojask.utils :as u])
  (:import [com.clojask.exception ExecutionException]))

(definterface MGroupIntf
  (final [])
  (getKeys [])
  (exists [key])
  (write [key msg write-index formatter] "mimic a bufferedwriter, add a row to a group")
  (getKey [key]))

(definterface MGroupJoinIntf
  (getKeyBoth [key])
  (delete [key]))

(deftype MGroup
         [^:unsynchronized-mutable groups]
  
  MGroupIntf

  (final
   [this]
   (set! groups (persistent! groups)))
  
  (getKeys
   [this]
  ;;  (println (keys groups))
   (keys groups))
  
  (exists
   [this key]
   (nil? (get groups key)))
  
  (write
   [this key msg write-index formatter]
   (if-let [group (get groups key)]
     (set! groups (assoc! groups key (conj! group (u/gets msg write-index))))
     (set! groups (assoc! groups key (transient [(u/gets msg write-index)])))))
  
  (getKey
   [this key]
   (persistent! (get groups key))))

(deftype MGroupJoin
         [^:unsynchronized-mutable groups
          ;; ^:unsynchronized-mutable unformat-groups
          ^:volatile-mutable _keys
          rolling]
  MGroupIntf
  (final
    [this]
    (let [tmp-keys (persistent! _keys)]
      ;; (if rolling
      ;;   (doseq [key (keys tmp-keys)]
      ;;     (set! groups (assoc! groups key (persistent! (get groups key))))
      ;;     (set! unformat-groups (assoc! unformat-groups key (persistent! (get unformat-groups key)))))
      (doseq [key (keys tmp-keys)]
        (set! groups (assoc! groups key (persistent! (get groups key)))))
      ;; )
      (set! _keys (transient tmp-keys)))
    (set! groups (persistent! groups))
  ;;  (println rolling)
  ;;  (println groups)
    ;; (set! unformat-groups (persistent! unformat-groups))
    )

  (getKeys
    [this]
    (keys groups))

  (exists
    [this key]
    (contains? _keys key))

  (write
    [this key msg write-index formatter]
    (if-let [group (get groups key)]
      (do
        (if rolling
          (set! groups (assoc! groups key (conj! group [(u/gets-format msg write-index formatter) (u/gets msg write-index)])))
          (set! groups (assoc! groups key (conj! group (u/gets-format msg write-index formatter))))))
      (do
        (if rolling
          (set! groups (assoc! groups key (transient [[(u/gets-format msg write-index formatter) (u/gets msg write-index)]])))
          (set! groups (assoc! groups key (transient [(u/gets-format msg write-index formatter)]))))
        (set! _keys (assoc! _keys key 1)))))

  (getKey
    [this key]
    (get groups key))
)


(deftype MGroupJoinOuter
         [^:unsynchronized-mutable groups
          ;; ^:unsynchronized-mutable unformat-groups
          ^:volatile-mutable _keys
          rolling]
  MGroupIntf
  (final
    [this]
    (set! _keys (persistent! _keys))
  ;;  (let [tmp-keys (persistent! _keys)]
  ;;     ;; (if rolling
  ;;     ;;   (doseq [key (keys tmp-keys)]
  ;;     ;;     (set! groups (assoc! groups key (persistent! (get groups key))))
  ;;     ;;     (set! unformat-groups (assoc! unformat-groups key (persistent! (get unformat-groups key)))))
  ;;    (doseq [key (keys tmp-keys)]
  ;;      (set! groups (assoc! groups key (persistent! (get groups key)))))
  ;;     ;; )
  ;;    (set! _keys (transient tmp-keys)))
  ;;  (set! groups (persistent! groups))
  ;;  (println rolling)
  ;;  (println groups)
    ;; (set! unformat-groups (persistent! unformat-groups))
    )

  (getKeys
    [this]
    (keys _keys))

  (exists
    [this key]
    (contains? groups key))

  (write
    [this key msg write-index formatter]
    (if-let [group (get groups key)]
      (set! groups (assoc! groups key (conj! group (u/gets-format msg write-index formatter))))
      (set! groups (assoc! groups key (transient [(u/gets-format msg write-index formatter)]))))
    (set! _keys (assoc! _keys key 1)))
  
  (getKey
    [this key]
    (persistent! (get groups key)))

  MGroupJoinIntf
  ;; (getKeyBoth
  ;;   [this key]
  ;;   (if (.exists this key) (get unformat-groups key)))

  (delete
    [this key]
    (set! _keys (dissoc! _keys key))))
