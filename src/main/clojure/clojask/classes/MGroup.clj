(ns clojask.classes.MGroup
  (:require [clojask.utils :as u]))

(definterface MGroupIntf
  (final [])
  (getKeys [])
  (exists [key])
  (write [key msg write-index formatter] "mimic a bufferedwriter, add a row to a group")
  (getKey [key]))

(definterface MGroupJoinIntf
  (delete [key]))

(deftype MGroup
         [^:unsynchronized-mutable groups]
  
  MGroupIntf

  (final
   [this]
   (set! groups (persistent! groups)))
  
  (getKeys
   [this]
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
      (doseq [key (keys tmp-keys)]
        (set! groups (assoc! groups key (persistent! (get groups key)))))
      ;; )
      (set! _keys (transient tmp-keys)))
    (set! groups (persistent! groups))
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

  (delete
    [this key]
    (set! _keys (dissoc! _keys key))))
