(ns clojask.heap
  (:refer-clojure :exclude [peek]))

(definterface HeapItf
  (setArr [arr] "change the heap array")
  (getArr [])
  (getComp []))

(deftype Heap [^:unsynchronized-mutable arr
               ^:unsynchronized-mutable comp]
  HeapItf
  (getArr
    [this]
    arr)
  (getComp
    [this]
    comp)
  (setArr
    [this newArr]
    (set! arr newArr)))

(defn parent
  [i]
  (int (Math/floor (/ (- i 1) 2))))

(defn left
  [i]
  (+ (* 2 i) 1))

(defn right
  [i]
  (+ (* 2 i) 2))

(defn swap
  "swap the value of i and j in heap arr"
  [heap i j]
  (let [val-i (get heap i)
        val-j (get heap j)
        heap (assoc! heap i val-j)
        heap (assoc! heap j val-i)]
    heap))

(defn top-down
  [arr comp]
  (let [size (count arr)]
    (loop [curr 0 arr arr]
      (let [l (left curr)
            r (right curr)]
        (if (>= l size)
          arr
          (if (>= r size)
            ;; only left exist
            (if (comp (get arr curr) (get arr l))
              arr
              (recur l (swap arr curr l)))
            ;; both left and right
            (let [c-l (comp (get arr curr) (get arr l))
                  c-r (comp (get arr curr) (get arr r))
                  l-r (comp (get arr l) (get arr r))]
              (if (and c-l c-r)
                arr
                (if (and c-l (not c-r))
                  (recur r (swap arr curr r))
                  (if l-r
                    (recur l (swap arr l curr))
                    (recur r (swap arr r curr))))))))))))

(defn bottom-up
  [arr comp]
  (let [size (count arr)]
    (loop [curr (dec size) arr arr]
      (let [p (parent curr)]
        (if (< p 0)
          arr
          (if (not (comp (get arr p) (get arr curr)))
            (recur p (swap arr p curr))
            arr))))))


;; external API
(defn heap
  ([comp]
   (Heap. (transient []) comp))
  ([comp value]
   (Heap. (transient [value]) comp)))

(defn get-size
  [heap]
  (count (.getArr heap)))

(defn peek
  [heap]
  (get (.getArr heap) 0))

(defn poll
  [heap]
  (let [size (get-size heap)
        comp (.getComp heap)]
    (if (> size 0)
      (let [arr (.getArr heap)
            arr (swap arr 0 (dec size))
            ret (get arr (dec size))
            arr (pop! arr)]
        (.setArr heap (top-down arr comp))
        ret)
      nil)))

(defn add
  [heap value]
  (let [comp (.getComp heap)
        arr (.getArr heap)
        arr (conj! arr value)]
    (.setArr heap (bottom-up arr comp))))