(ns circlecast.fdb.hospital
  (:require [circlecast.fdb.constructs :as impl]
            [circlecast.core :as core]
            [circlecast.fdb.operations :as ops]
            [circlecast.fdb.graph :as G]
            [circlecast.fdb.query :as Q]
            [hazel-atom.core :as hza]
            [clojure.set :refer (union difference)])
  (:import (com.hazelcast.core Hazelcast)))

(defonce db-name "hos12")
(defonce hz-atom-ref
  (-> (Hazelcast/newHazelcastInstance)
      .getCPSubsystem
      (.getAtomicReference db-name)))

(def get-db-conn*
  (partial core/get-db-conn
           (partial impl/make-db
                    (partial hza/hz-atom hz-atom-ref))))

(core/drop-db-conn db-name)

(def hospital-db (get-db-conn* db-name))

(defonce basic-kinds
  [:test/bp-systolic
   :test/bp-diastolic
   :test/temperature
   :person/patient
   :person/doctor])

(defn make-patient
  [id address symptoms]
  (-> (impl/make-entity id)
      (core/with-attributes
        [:patient/kind :person/patient :db/ref]
        [:patient/city address :string ]
        [:patient/tests #{} :db/ref :cardinality :db/multiple]
        [:patient/symptoms (set symptoms) :string :cardinality :db/multiple])))

(defn make-test
  [t-id tests-map types]
  (let [ent (impl/make-entity t-id)]
    (->> tests-map
         (map (juxt first  ; attr-name
                    second ; attr value
                    #(get types (first %) :number))) ; attr type
         (apply core/with-attributes ent))

    #_(reduce #(impl/add-attr %1 (impl/make-attr (first %2) ;attr-name
                                               (second %2) ; attr value
                                               (get types (first %2) :number) ; attr type
                                               ))
            ent tests-map)))

(defn add-machine
  [id nm]
  (core/transact! hospital-db
    (ops/add-entity
      (-> (impl/make-entity id)
          (core/with-attributes [:machine/name nm :string])))))


(defn add-patient 
  [id address symptoms]
  (core/transact! hospital-db
    (ops/add-entity
      (make-patient id address symptoms))))

(defn add-test-results-to-patient 
  [pat-id test-result]
  (let [test-id (:id test-result)]
    (core/transact! hospital-db (ops/add-entity test-result))
    (core/transact! hospital-db (ops/update-entity pat-id :patient/tests #{test-id} :db/add))))

(comment

  ;; world setup
  (core/transact! hospital-db  (ops/add-entities (map impl/make-entity basic-kinds)))

  (add-patient :pat1 "London" ["fever" "cough"])
  (add-patient :pat2 "London" ["fever" "cough"])

  (add-machine :1machine1 "M11")
  (add-machine :2machine2 "M222")

  (add-test-results-to-patient
    :pat1 (make-test :t2-pat1
                     {:test/bp-systolic 170 :test/bp-diastolic 80 :test/machine :2machine2 }
                     {:test/machine :db/ref} ))
  (add-test-results-to-patient
    :pat2  (make-test :t4-pat2
                      {:test/bp-systolic 170 :test/bp-diastolic 90 :test/machine :1machine1}
                      {:test/machine :db/ref} ))
  (add-test-results-to-patient
    :pat2  (make-test :t3-pat2
                      {:test/bp-systolic 140 :test/bp-diastolic 80 :test/machine :2machine2}
                      {:test/machine :db/ref} ))

  (core/transact! hospital-db  (ops/update-entity :pat1 :patient/symptoms #{"cold sweat" "sneeze"} :db/reset-to))
  (core/transact! hospital-db  (ops/update-entity :pat1 :patient/tests #{:t2-pat1} :db/add))
  ;  (transact hospital-db (remove-entity :t2-pat1))

  (defn- keep-on-equals [a b]
    (when (= a b) a))

  (Q/q @hospital-db {:find [?id ?k ?b]
                     :where [[ ?id :test/bp-systolic (> 200 ?b)]
                             [ ?id :test/bp-diastolic ?k]]} )
  ;; (defn apply-order
  ;;   "Order the unified result entry's pairs (each is a pair with symbol and value) to be by the order of symbols"
  ;;   [order-symbols unified-res-entry]
  ;;   ;(println order-symbols)
  ;;  ; (println  unified-res-entry)
  ;;   (let [sym-to-entry-item (reduce #(assoc %1 (first %2) %2) {}  (first unified-res-entry))
  ;;         ]
  ;;     (map #(%1 %2)  (repeat sym-to-entry-item) order-symbols)))


  ;(def qw
  (Q/q @hospital-db {:find [?id ?a ?v]
                     :where [[ (= ?id :pat1) (= ?a :patient/city) ?v]
                             ]})
  ;)
  ;qw
  ;(second qw)
  ;(map (partial apply-order ["?id" "?a" "?v"]) qw)
  ;;([("?e" :t2-pat1) ("?k" 80) ("?b" 170)] [("?e" :t4-pat2) ("?k" 90) ("?b" 170)] [("?e" :t3-pat2) ("?k" 80) ("?b" 140)])

  ;;(def qq
  (Q/q @hospital-db {:find [?id ?a ?b]
                     :where [[?id ?a (> 200 ?b)]]})
  ;; )

  ;;(defn apply-order [order-symbols unified-res-entry]
  ;; (let [sym-to-entry-item (reduce #(assoc %1 (first %2) %2) {}  unified-res-entry)
  ;;      ]  (vec(map #(%1 %2)  (repeat sym-to-entry-item) order-symbols )))
  ;; )

  ;(map (partial apply-order ["?a" "?b" "?id"]) qq )

  ;;([("?a" :test/bp-diastolic) ("?b" 80) ("?a" :test/bp-systolic) ("?b" 140)]
  ;;  [("?a" :test/bp-diastolic) ("?b" 80) ("?a" :test/bp-systolic) ("?b" 170)]
  ;;  [("?a" :test/bp-diastolic) ("?b" 90) ("?a" :test/bp-systolic) ("?b" 170)])

  ; (defmacro symbol-col-to-vec [coll] (vec (map str coll)))

  ;(symbol-col-to-vec [?a1 ?f ?c ])


  (Q/q @hospital-db {:find [?k ?e ?b]
                     :where [[ ?e :test/bp-systolic (> 160 ?b)]
                             [ ?e :test/bp-diastolic ?k] ]})
  ;;([("?e" :t3-pat2) ("?k" 80) ("?b" 140)])

  (core/evolution-of @hospital-db :pat1 :patient/symptoms)
  (core/evolution-of @hospital-db :pat1 :patient/tests)

  (take 7 (G/traverse-db  :pat2 @hospital-db :bfs :incoming))


  )






