(ns clojask.onyx-comps
  (:require [clojask.clojask-aggre :as aggre]
            [clojask.clojask-groupby :as groupby]
            [clojask.clojask-input :as input]
            [clojask.clojask-join :as join]
            [clojask.clojask-output :as output]
            [clojask.join :refer [defn-join]]
            ;; [clojask.utils :refer [u/eval-res u/eval-res-ne u/filter-check]]
            [clojask.utils :as u]
            [onyx.api :refer :all]
            [onyx.test-helper :refer [feedback-exception!]] ;; [tech.v3.dataset :as ds]
)
  (:import [com.clojask.exception ExecutionException]
           [java.io FileReader]))


;; sample workflow
;;
;; [[:in :sample-worker1]
;;  [:in :sample-worker2]
;;  [:sample-worker1 :output]
;;  [:sample-worker2 :output]]

(def id (java.util.UUID/randomUUID))

(defn workflow-gen
  "Generate workflow for running Onyx"
  [num-work]
  (def workflow []) ;; initialisation

  ;; for loop for input edges
  (doseq [x (range 1 (+ num-work 1))]
    (let [worker-name (keyword (str "sample-worker" x))]
          (def workflow (conj workflow [:in worker-name]
              ))))

  ;; for loop for output edges
  (doseq [x (range 1 (+ num-work 1))]
    (let [worker-name (keyword (str "sample-worker" x))]
          (def workflow (conj workflow [worker-name :output]
              ))))

  ;; (println workflow) ; !!debugging
  )


;; (defn sample-worker
;;   [segment]
;;   ;; (println segment)
;;   (:id segment)
;;   ;; (update-in segment [:map] (fn [n] (assoc n :first (:id segment))))
;;   )

(def dataframe (atom nil))


(defn worker-func-gen
  [df exception index]
  (reset! dataframe df)
  (let [operations (.getDesc (:col-info (deref dataframe)))
        types (.getType (:col-info (deref dataframe)))
        formats (.getFormatter (:col-info (deref dataframe)))
        filters (.getFilters (:row-info df))
        indices index]
    ;; (println indices)
    (if exception
      (defn worker-func
        [seg]
        (let [id (:id seg)
              data (:d seg)] ;; -1 is very important here!
          {:id id :d (for [row data]
                       (if (u/filter-check filters types row)
                         (mapv (fn [_] (u/eval-res row types formats operations _)) indices)
                         nil))}
          ;; (if (u/filter-check filters types data)
          ;;   {:id id :d (mapv (fn [_] (u/eval-res data types formats operations _)) indices)}
          ;;   {:id id})
          ))
      (defn worker-func
        [seg]
        (let [id (:id seg)
              data (:d seg)]
          ;; (if (u/filter-check filters types data)
          ;;   {:id id :d (mapv (fn [_] (u/eval-res-ne data types formats operations _)) indices)}
          ;;   {:id id})
          {:id id :d (for [row data]
                       (if (u/filter-check filters types row)
                         (mapv (fn [_] (u/eval-res-ne row types formats operations _)) indices)
                         nil))}
          ))))
  )

(defn worker-func-gen-format
  [df exception index]
  (reset! dataframe df)
  (let [operations (.getDesc (:col-info (deref dataframe)))
        types (.getType (:col-info (deref dataframe)))
        formats (.getFormatter (:col-info (deref dataframe)))
        filters (.getFilters (:row-info df))
        indices index]
    ;; (println indices)
    (if exception
      (defn worker-func
        [seg]
        (let [id (:id seg)
              data (:d seg)] ;; -1 is very important here!
          {:id id :d (for [row data]
                       (if (u/filter-check filters types row)
                         (mapv (fn [_] ((or (get formats _) str) (u/eval-res row types formats operations _))) indices)
                         nil))}
          ;; (if (u/filter-check filters types data)
          ;;   {:id id :d (mapv (fn [_] ((or (get formats _) str) (u/eval-res data types formats operations _))) indices)}
          ;;   {:id id})
          ))
      (defn worker-func
        [seg]
        (let [id (:id seg)
              data (:d seg)]
          ;; (if (u/filter-check filters types data)
          ;;   {:id id :d (mapv (fn [_] ((or (get formats _) str) (u/eval-res-ne data types formats operations _))) indices)}
          ;;   {:id id})
          {:id id :d (for [row data]
                       (if (u/filter-check filters types row)
                         (mapv (fn [_] ((or (get formats _) str) (u/eval-res-ne row types formats operations _))) indices)
                         nil))}
          )))))

(defn catalog-gen
  "Generate the catalog for running Onyx"
  [num-work batch-size]
  ;; initialisation
  (def catalog [])

  ;; input
  (def catalog 
    (conj catalog
     {:onyx/name :in
      :onyx/plugin :clojask.clojask-input/input
      :onyx/type :input
      :onyx/medium :seq
      :seq/checkpoint? true
      :onyx/batch-size batch-size
      :onyx/max-peers 1
      :input/doc "Reads segments from a core.async channel"}))

    ;; for loop for sample workers
    (doseq [x (range 1 (+ num-work 1))]
      (let [worker-name (keyword (str "sample-worker" x))
            worker-function (keyword "clojask.onyx-comps" "worker-func")]
            (def catalog 
              (conj catalog
               {:onyx/name worker-name
                :onyx/fn worker-function
                :onyx/type :function
                :onyx/batch-size batch-size
                :worker/doc "This is a worker node"}
                ))))
    
    ;; output
    (def catalog
      (conj catalog
      {:onyx/name :output
        :onyx/plugin :clojask.clojask-output/output
        :onyx/type :output
        :onyx/medium :core.async  ;; this is maked up
        :onyx/max-peers 1
        :onyx/batch-size batch-size
        :output/doc "Writes segments to the file"}))

    ;; (println catalog) ;; !! debugging
    )

(defn catalog-aggre-gen
  "Generate the catalog for running Onyx"
  [num-work batch-size]
  ;; initialisation
  (def catalog [])

  ;; input
  (def catalog
    (conj catalog
          {:onyx/name :in
           :onyx/plugin :clojask.clojask-input/input
           :onyx/type :input
           :onyx/medium :seq
           :seq/checkpoint? true
           :onyx/batch-size batch-size
           :onyx/max-peers 1
           :input/doc "Reads segments from a core.async channel"}))

    ;; for loop for sample workers
  (doseq [x (range 1 (+ num-work 1))]
    (let [worker-name (keyword (str "sample-worker" x))
          worker-function (keyword "clojask.onyx-comps" "worker-func")]
      (def catalog
        (conj catalog
              {:onyx/name worker-name
               :onyx/fn worker-function
               :onyx/type :function
               :onyx/batch-size batch-size
               :worker/doc "This is a worker node"}))))

    ;; output
  (def catalog
    (conj catalog
          {:onyx/name :output
           :onyx/plugin :clojask.clojask-aggre/output
           :onyx/type :output
           :onyx/medium :core.async  ;; this is maked up
           :onyx/max-peers 1
           :onyx/batch-size batch-size
           :output/doc "Writes segments to the file"}))

    ;; (println catalog) ;; !! debugging
  )

(defn catalog-groupby-gen
  "Generate the catalog for running Onyx"
  [num-work batch-size]
  ;; initialisation
  (def catalog [])

  ;; input
  (def catalog
    (conj catalog
          {:onyx/name :in
           :onyx/plugin :clojask.clojask-input/input
           :onyx/type :input
           :onyx/medium :seq
           :seq/checkpoint? true
           :onyx/batch-size batch-size
           :onyx/max-peers 1
           :input/doc "Reads segments from a core.async channel"}))

    ;; for loop for sample workers
  (doseq [x (range 1 (+ num-work 1))]
    (let [worker-name (keyword (str "sample-worker" x))
          worker-function (keyword "clojask.onyx-comps" "worker-func")]
      (def catalog
        (conj catalog
              {:onyx/name worker-name
               :onyx/fn worker-function
               :onyx/type :function
               :onyx/batch-size batch-size
               :worker/doc "This is a worker node"}))))

    ;; output
  (def catalog
    (conj catalog
          {:onyx/name :output
           :onyx/plugin :clojask.clojask-groupby/groupby
           :onyx/type :output
           :onyx/medium :core.async  ;; this is maked up
           :onyx/max-peers 1
           :onyx/batch-size batch-size
           :output/doc "Writes segments to the file"}))

    ;; (println catalog) ;; !! debugging
  )

(defn catalog-join-gen
  "Generate the catalog for running Onyx"
  [num-work batch-size]
  ;; initialisation
  (def catalog [])

  ;; input
  (def catalog
    (conj catalog
          {:onyx/name :in
           :onyx/plugin :clojask.clojask-input/input
           :onyx/type :input
           :onyx/medium :seq
           :seq/checkpoint? true
           :onyx/batch-size batch-size
           :onyx/max-peers 1
           :input/doc "Reads segments from a core.async channel"}))

    ;; for loop for sample workers
  (doseq [x (range 1 (+ num-work 1))]
    (let [worker-name (keyword (str "sample-worker" x))
          worker-function (keyword "clojask.onyx-comps" "worker-func")]
      (def catalog
        (conj catalog
              {:onyx/name worker-name
               :onyx/fn worker-function
               :onyx/type :function
               :onyx/batch-size batch-size
               :worker/doc "This is a worker node"}))))

    ;; output
  (def catalog
    (conj catalog
          {:onyx/name :output
           :onyx/plugin :clojask.clojask-join/join
           :onyx/type :output
           :onyx/medium :core.async  ;; this is maked up
           :onyx/max-peers 1
           :onyx/batch-size batch-size
           :output/doc "Writes segments to the file"}))

    ;; (println catalog) ;; !! debugging
  )


(defn inject-in-reader [event lifecycle]
  (let [
        ;; path (:buffered-reader/filename lifecycle)
        ;; tmp (println path)
        ;; rdr (if (= path nil) nil (FileReader. path))
        ;; csv-data (csv/read-csv (BufferedReader. rdr))
        ]
    {
    ;;  :seq/rdr rdr
    ;;  :seq/seq (map zipmap ;; make the first row as headers and the following rows as values in a map structure e.g. {:tic AAPL} 
    ;;                (->> (first csv-data) ;; take the first row of the csv-data
    ;;                     (cons "clojask-id")
    ;;                     (map keyword) ;; make the header be the "key" in the map 
    ;;                     repeat)      ;; repeat the process for all the headers
    ;;                (map cons (iterate inc 1) (rest csv-data)))
    ;;  :seq/filters (:clojask/filters lifecycle)
    ;;  :seq/types (:clojask/types lifecycle)
     }
    ))

(defn close-reader [event lifecycle]
  (if (not= (:seq/rdr event) nil)
   (.close (:seq/rdr event))))

;; (defn inject-out-writer [event lifecycle]
;;   (let [wrt (BufferedWriter. (FileWriter. (:buffered-writer/filename lifecycle)))]
;;     {:seq/wrt wrt}))

;; (defn close-writer [event lifecycle]
;;   (.close (:clojask/wtr event)))

;; (def writer-calls
;;   {:lifecycle/before-task-start inject-out-writer
;;    :lifecycle/after-task-stop close-writer})

(def in-calls
  {:lifecycle/before-task-start inject-in-reader
   :lifecycle/after-task-stop close-reader})


(defn lifecycle-gen
  [source dist order select]
  (def lifecycles
    [{:lifecycle/task :in
      :buffered-reader/filename nil
      ;; :clojask/filters (.getFilters (:row-info (deref dataframe)))
      ;; :clojask/types (.getType (:col-info (deref dataframe)))
      :lifecycle/calls ::in-calls}
     {:lifecycle/task :in
      :lifecycle/calls :clojask.clojask-input/reader-calls}
     {:lifecycle/task :output
      :buffered-wtr/filename dist
      :order order
      :indices select
      :lifecycle/calls :clojask.clojask-output/writer-calls}]))

(defn lifecycle-aggre-gen
  [source dist]
  (def lifecycles
    [{:lifecycle/task :in
      :buffered-reader/filename (if (fn? source) nil source)
      ;; :clojask/filters (.getFilters (:row-info (deref dataframe)))
      ;; :clojask/types (.getType (:col-info (deref dataframe)))
      :lifecycle/calls ::in-calls}
     {:lifecycle/task :in
      :lifecycle/calls :clojask.clojask-input/reader-calls}
     {:lifecycle/task :output
      :buffered-wtr/filename dist
      ;; :order order
      :lifecycle/calls :clojask.clojask-aggre/writer-calls}]))

(defn lifecycle-groupby-gen
  [source dist keys key-index]
  (def lifecycles
    [{:lifecycle/task :in
      :buffered-reader/filename (if (fn? source) nil source)
      :lifecycle/calls ::in-calls}
     {:lifecycle/task :in
      :lifecycle/calls :clojask.clojask-input/reader-calls}
     {:lifecycle/task :output
      :buffered-wtr/filename dist
      ;; :clojask/groupby-keys keys
      :clojask/key-index key-index
      :lifecycle/calls :clojask.clojask-groupby/writer-aggre-calls}]))

(defn lifecycle-join-gen
  [source dist a b a-keys b-keys a-roll b-roll join-type]
  (def lifecycles
    [{:lifecycle/task :in
      :buffered-reader/filename (if (fn? source) nil source)
      :lifecycle/calls ::in-calls}
     {:lifecycle/task :in
      :lifecycle/calls :clojask.clojask-input/reader-calls}
     {:lifecycle/task :output
      :buffered-wtr/filename dist
      ;; :clojask/a-keys a-keys
      ;; :clojask/b-keys b-keys 
      :clojask/a-roll a-roll
      :clojask/b-roll b-roll
      :clojask/a-map (.getKeyIndex (.col-info a)) 
      :clojask/b-map (.getKeyIndex (.col-info b))
      :clojask/join-type join-type
      :lifecycle/calls :clojask.clojask-join/writer-join-calls}]))

;; (def num-workers (atom 1))

;; (defn rem0?
;;   [event old-segment new-segment all-new-segment]
;;   ;; (spit "resources/debug.txt" (str new-segment "\n") :append true)
;;   (= (mod (:id new-segment) (deref num-workers)) 0))

;; (defn rem1?
;;   [event old-segment new-segment all-new-segment]
;;   (= (mod (:id new-segment) (deref num-workers)) 1))

;; (defn rem2?
;;   [event old-segment new-segment all-new-segment]
;;   (= (mod (:id new-segment) (deref num-workers)) 2))

;; (defn rem3?
;;   [event old-segment new-segment all-new-segment]
;;   (= (mod (:id new-segment) (deref num-workers)) 3))

;; (defn rem4?
;;   [event old-segment new-segment all-new-segment]
;;   (= (mod (:id new-segment) (deref num-workers)) 4))

;; (defn rem5?
;;   [event old-segment new-segment all-new-segment]
;;   (= (mod (:id new-segment) (deref num-workers)) 5))

;; (defn rem6?
;;   [event old-segment new-segment all-new-segment]
;;   (= (mod (:id new-segment) (deref num-workers)) 6))

;; (defn rem7?
;;   [event old-segment new-segment all-new-segment]
;;   (= (mod (:id new-segment) (deref num-workers)) 7))

;; (defn rem8?
;;   [event old-segment new-segment all-new-segment]
;;   (= (mod (:id new-segment) (deref num-workers)) 8))


;; [{:flow/from :in
;;   :flow/to [:sample-worker1]
;;   :flow/predicate :clojask.onyx-comps/rem0?
;;   :flow/doc ""}
;;  {:flow/from :in
;;   :flow/to [:sample-worker2]
;;   :flow/predicate :clojask.onyx-comps/rem1?
;;   :flow/doc ""}]

;; (defn predicate-function
;;   [event old-segment new-segment all-new-segment id]
;;   (= (mod (:id new-segment) (deref num-workers)) id))

(defn flow-cond-gen
  "Generate the flow conditions for running Onyx"
  [num-work]
  ;; (reset! num-workers num-work)
  (def flow-conditions []) ;; initialisation
  (def predicate-funcs [])
  ;; for loop for sample workers
  (doseq [x (range 1 (+ num-work 1))]
    (let [worker-name (keyword (str "sample-worker" x))
          predicate-function (keyword "clojask.onyx-comps" (str "rem" (- x 1) "?"))
          ;; predicate-function (fn [event old-segment new-segment all-new-segment]
          ;;                      (= (mod (:id new-segment) num-work) (- x 1)))
          ]
      ;; (def predicate-funcs (conj predicate-funcs predicate-function))
      (intern 'clojask.onyx-comps (symbol (str "rem" (- x 1) "?")) (fn [event old-segment new-segment all-new-segment]
                                                                     (= (mod (:id new-segment) num-work) (- x 1))))
      (def flow-conditions
        (conj flow-conditions
              {:flow/from :in
               :flow/to [worker-name]
               :flow/predicate predicate-function
               :worker/doc "This is a flow condition"}))))

  ;; (println flow-conditions) ;; !! debugging
  )

;; Components started by config-env. Kept as top-level vars so shutdown can
;; tear down whatever a partial startup managed to create.
(def env nil)
(def peer-group nil)
(def v-peers nil)

(defn config-env
  []
  ;; Clear the previous run first, so a failure below leaves only the
  ;; components that were actually started in this run.
  (def env nil)
  (def peer-group nil)
  (def v-peers nil)
  (def env-config
    {:zookeeper/address "127.0.0.1:2188"
     :zookeeper/server? true
     :zookeeper.server/port 2188
     :onyx/tenancy-id id
     :onyx.log/file ".clojask/clojask.log"})

  (def peer-config
    {:zookeeper/address "127.0.0.1:2188"
     :onyx/tenancy-id id
     :onyx.peer/job-scheduler :onyx.job-scheduler/balanced
     :onyx.messaging/impl :aeron
     :onyx.messaging/peer-port 40200
     :onyx.messaging/bind-addr "localhost"
     :onyx.log/file ".clojask/clojask.log"})

  (def env (onyx.api/start-env env-config))

  (def peer-group (onyx.api/start-peer-group peer-config))

  (def n-peers (count (set (mapcat identity workflow))))

  (def v-peers (onyx.api/start-peers n-peers peer-group)))

(defn shutdown
  "Tear down whatever config-env managed to start, in reverse order. Every
   step is attempted even if an earlier one fails, so a startup that dies
   after ZooKeeper is up (for example in the peer group) still releases the
   ZooKeeper port instead of poisoning every later compute in this JVM. The
   first error, if any, is rethrown once all steps have run."
  []
  (let [errors (volatile! [])
        attempt (fn [f] (try (f) (catch Exception e (vswap! errors conj e))))]
    (doseq [v-peer v-peers]
      (attempt #(onyx.api/shutdown-peer v-peer)))
    (when peer-group
      (attempt #(onyx.api/shutdown-peer-group peer-group)))
    (when env
      (attempt #(onyx.api/shutdown-env env)))
    (def v-peers nil)
    (def peer-group nil)
    (def env nil)
    (when-let [e (first @errors)]
      (throw e))))

(defn- stage-error
  [stage e]
  (ExecutionException.
   (format "[%s] Refer to .clojask/clojask.log for detailed information. (original error: %s)"
           stage (.getMessage e))
   e))

(defn submit-and-wait
  "Submit one job to the running environment and block until it has
   finished, rethrowing the job's own exception if it failed. The job map
   supplies :catalog, :lifecycles and :flow-conditions; the workflow is the
   one config-env sized the peers for."
  [job]
  (let [submission (onyx.api/submit-job peer-config
                                        (assoc job
                                               :workflow workflow
                                               :task-scheduler :onyx.task-scheduler/balanced))
        job-id (:job-id submission)]
    (assert job-id "Job was not successfully submitted")
    (feedback-exception! peer-config job-id)))

(defn run-job
  "Run one job inside with-onyx-env. prepare! does the per-job setup and
   returns the job map for submit-and-wait. Failures are tagged with the
   stage they came from; with-onyx-env shuts the environment down."
  [stage prepare!]
  (let [job (try
              (prepare!)
              (catch Exception e
                (throw (stage-error (str "preparing stage (" stage ")") e))))]
    (try
      (submit-and-wait job)
      (catch Exception e
        (throw (stage-error (str "submit-to-onyx stage (" stage ")") e))))))

(defn with-onyx-env
  "Start the Onyx environment (embedded ZooKeeper, peer group and one peer
   per task) for num-work workers, call f, and shut everything down again
   whether or not f threw. This is the only place that starts or stops the
   environment, so every driver gets the same partial-startup handling."
  [stage num-work f]
  (try
    (workflow-gen num-work)
    (config-env)
    (catch Exception e
      (try (shutdown) (catch Exception _))
      (throw (stage-error (str "preparing stage (" stage ")") e))))
  (try
    (f)
    (catch Exception e
      (try (shutdown) (catch Exception _))
      (throw (if (instance? ExecutionException e)
               e
               (stage-error (str "run stage (" stage ")") e)))))
  (try
    (shutdown)
    (catch Exception e
      (throw (stage-error (str "terminate-node stage (" stage ")") e))))
  "success")

(defn- job-spec
  "The job map built by the *-gen functions of this namespace."
  []
  {:catalog catalog :lifecycles lifecycles :flow-conditions flow-conditions})

(defn start-onyx
  "start the onyx cluster with the specification inside dataframe"
  [num-work batch-size dataframe dist exception order index melt out]
  (with-onyx-env "compute" num-work
    (fn []
      (run-job "compute"
               (fn []
                 (worker-func-gen-format dataframe exception index)
                 (catalog-gen num-work batch-size)
                 (lifecycle-gen (.getFunc dataframe) dist order index)
                 (flow-cond-gen num-work)
                 (input/inject-dataframe dataframe)
                 (output/inject-dataframe dataframe out)
                 (output/inject-melt melt)
                 (job-spec))))))

(defn start-onyx-aggre-only
  "start the onyx cluster with the specification inside dataframe"
  [num-work batch-size dataframe dist exception aggre-func index select out]
  (with-onyx-env "aggregate" num-work
    (fn []
      (run-job "aggregate"
               (fn []
                 (worker-func-gen dataframe exception index)
                 (catalog-aggre-gen num-work batch-size)
                 (lifecycle-aggre-gen (.getFunc dataframe) dist)
                 (flow-cond-gen num-work)
                 (input/inject-dataframe dataframe)
                 (aggre/inject-dataframe dataframe aggre-func select out)
                 (job-spec))))))

(defn start-onyx-groupby
  "start the onyx cluster with the specification inside dataframe\n
   @format: if format the value before writing to file. For procedures that will need to compare / use the actual
   value of each element, should be set to false, such as aggregate, rolling join. For others, should be set to
   false to avoid repeated formatting
   "
  [num-work batch-size dataframe dist groupby-keys groupby-index exception & {:keys [format] :or {format false}}]
  (with-onyx-env "groupby" num-work
    (fn []
      (run-job "groupby"
               (fn []
                 (worker-func-gen dataframe exception (vec (take (count (.getKeyIndex (.col-info dataframe))) (iterate inc 0))))
                 (catalog-groupby-gen num-work batch-size)
                 ;; use of dist from here is deprecated
                 (lifecycle-groupby-gen (.getFunc dataframe) (if (string? dist) dist nil) groupby-keys (.getKeyIndex (.col-info dataframe)))
                 (flow-cond-gen num-work)
                 (input/inject-dataframe dataframe)
                 (groupby/inject-dataframe dataframe groupby-keys groupby-index dist format)
                 (job-spec))))))

(defn start-onyx-join
  "start the onyx cluster with the specification inside dataframe"
  [num-work batch-size dataframe b source dist exception a-keys b-keys a-roll b-roll join-type limit a-index b-index b-format write-index out]
  ;; dataframe means a
  (with-onyx-env "join" num-work
    (fn []
      (run-job "join"
               (fn []
                 (worker-func-gen dataframe exception (take (count (.getKeyIndex (:col-info dataframe))) (iterate inc 0)))
                 (catalog-join-gen num-work batch-size)
                 (lifecycle-join-gen (.getFunc dataframe) dist dataframe b a-keys b-keys a-roll b-roll join-type)
                 (flow-cond-gen num-work)
                 (input/inject-dataframe dataframe)
                 (join/inject-dataframe dataframe b a-keys b-keys a-index b-index write-index b-format out)
                 (defn-join join-type (or limit (fn [a b] true)) source)
                 (job-spec))))))


;; !! debugging
(defn- -main
  [& args]
  ;; (catalog-gen 2 10)
  ;; (workflow-gen 2)
  ;; (flow-cond-gen 2)
  ;; (start-onyx 2 10 )
  )