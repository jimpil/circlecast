(ns circlecast.fdb.core
  (:require [clojure.set :as CS]
            [circlecast.util :as ut]
            [circlecast.fdb.constructs :as impl]
            [circlecast.fdb.storage :as storage]))

(defn- next-ts [db]
  (inc (:present db)))

(defn- next-id
  "returns a pair composed of the id to use for the given entity and the next free running id in the database"
  [ent]
  (let [ent-id (:id ent)]
    (if (= ent-id :db/no-id-yet)
      (ut/uuid!)
      ent-id)))

(defn- update-attr-value
  "updating the attribute value based on the kind of the operation,
   the cardinality defined for this attribute and the given value"
  [attr value operation]
  (cond
    (impl/single? attr)        (assoc attr :value #{value})
    ; now we're talking about an attribute of multiple values
    (= :db/reset-to operation) (assoc attr :value value)
    (= :db/add operation)      (assoc attr :value (CS/union (:value attr)  value))
    (= :db/remove operation)   (assoc attr :value (CS/difference (:value attr) value))))

(defn- update-creation-ts
  "updates the timestamp value of all the attributes of an entity to the given timestamp"
  [ent ts-val]
  (reduce #(assoc-in %1 [:attrs %2 :current] ts-val) ent (keys (:attrs ent))))

(defn- update-entry-in-index
  [index path operation]
  (let [update-path (butlast path)
        update-value (last path)
        to-be-updated-set (get-in index update-path #{})]
    (assoc-in index update-path (conj to-be-updated-set update-value) )))

(defn- update-attr-in-index
  [index ent-id attr-name target-val operation]
  (let [colled-target-val (ut/collify target-val)
        update-entry-fn (fn [indx vl] (update-entry-in-index indx ((impl/from-eav index) ent-id attr-name vl) operation))]
    (reduce update-entry-fn index colled-target-val)))

(defn- add-entity-to-index [ent layer ind-name]
  (let [ent-id (:id ent)
        index (ind-name layer)
        all-attrs  (vals (:attrs ent))
        relevant-attrs (filter #((impl/usage-pred index) %) all-attrs)
        add-in-index-fn (fn [ind attr] (update-attr-in-index ind ent-id (:name attr) (:value attr) :db/add))]
    (assoc layer ind-name (reduce add-in-index-fn index relevant-attrs))))

(defn- fix-new-entity
  [db ent]
  (let [ent-id (next-id ent)
        layer-index (next-ts db)]
    (-> ent
        (assoc :id ent-id)
        (update-creation-ts layer-index))))

(defn- append-layer
  ([db layer]
   (append-layer db layer nil))
  ([db layer next-top-id]
   (let [layer (assoc layer :tx-ts (ut/now-instant!))]
     (cond-> db
           true (update :layers conj layer)
           next-top-id (assoc :top-id next-top-id)))))

(defn add-entity
  [db ent]
  (let [fixed-ent (fix-new-entity db ent)
        layer-with-updated-storage (-> db impl/current-layer (update :storage storage/write-entity fixed-ent))
        add-fn (partial add-entity-to-index fixed-ent)
        new-layer (reduce add-fn layer-with-updated-storage (keys impl/indices))]
    (append-layer db new-layer)))

(defn add-entities
  [db ents-seq]
  (reduce add-entity db ents-seq))

(defn- update-attr-modification-time
  [attr new-ts]
  (assoc attr :current new-ts
              :previous (:current attr)))

(defn- update-attr
  [attr new-val new-ts operation]
  {:pre  [(if (impl/single? attr)
            (contains? #{:db/reset-to :db/remove} operation)
            (contains? #{:db/reset-to :db/add :db/remove} operation))]}
  (-> attr
      (update-attr-modification-time new-ts)
      (update-attr-value new-val operation)))

(defn- remove-entry-from-index
  [index path]
  (let [path-head (first path)
        path-to-items (butlast path)
        val-to-remove (last path)
        old-entries-set (get-in index path-to-items)]
    (cond
      (not (contains?  old-entries-set val-to-remove)) index ; the set of items does not contain the item to remove, => nothing to do here
      (= 1 (count old-entries-set))  (update index path-head dissoc (second path)) ; a path that splits at the second item - just remove the unneeded part of it
      :else (update-in index path-to-items disj val-to-remove))))

(defn- remove-entries-from-index
  [ent-id operation index attr]
  (if (= operation :db/add)
    index
    (let  [attr-name (:name attr)
           datom-vals (ut/collify (:value attr))
           paths (map #((impl/from-eav index) ent-id attr-name %) datom-vals)]
      (reduce remove-entry-from-index index paths))))

(defn- update-index
  [ent-id old-attr target-val operation layer ind-name]
  (if ((impl/usage-pred (get-in layer [ind-name])) old-attr)
    (let [index (ind-name layer)
          cleaned-index (remove-entries-from-index  ent-id operation index old-attr)
          updated-index  (if (= operation :db/remove)
                           cleaned-index
                           (update-attr-in-index cleaned-index ent-id  (:name old-attr) target-val operation))]
      (assoc layer ind-name updated-index))
    layer))

(defn- put-entity
  [storage e-id new-attr]
  (-> storage
      (storage/get-entity e-id)
      (assoc-in [:attrs (:name new-attr)] new-attr)))

(defn- update-layer
  [layer ent-id old-attr updated-attr new-val operation]
  (let [updated-entity (-> layer :storage (put-entity ent-id updated-attr))
        new-layer (reduce (partial update-index ent-id old-attr new-val operation)
                          layer (keys impl/indices))]
    (update new-layer :storage storage/write-entity updated-entity)))

(defn update-entity
  ([db ent-id attr-name new-val]
   (update-entity db ent-id attr-name new-val :db/reset-to))
  ([db ent-id attr-name new-val operation]
   (let [update-ts (next-ts db)
         layer (impl/current-layer db)
         attr (impl/attr-at db ent-id attr-name)
         updated-attr (update-attr attr new-val update-ts operation)
         fully-updated-layer (update-layer layer ent-id attr updated-attr new-val operation)]
     (append-layer db fully-updated-layer))))

(defn- remove-entity-from-index [ent layer ind-name]
  (let [ent-id (:id ent)
        index (ind-name layer)
        all-attrs  (vals (:attrs ent))
        relevant-attrs (filter #((impl/usage-pred index) %) all-attrs )
        remove-from-index-fn (partial remove-entries-from-index  ent-id  :db/remove)]
    (assoc layer ind-name (reduce remove-from-index-fn index relevant-attrs))))

(defn- reffing-to [e-id layer]
  (let [vaet (:VAET layer)]
    (for [[attr-name reffing-set] (e-id vaet)
          reffing reffing-set]
      [reffing attr-name])))

(defn- remove-back-refs [db e-id layer]
  (let [reffing-datoms (reffing-to e-id layer)
        remove-fn (fn [d [e a]] (update-entity db e a e-id :db/remove))
        clean-db (reduce remove-fn db reffing-datoms)]
    (impl/current-layer clean-db)))

(defn remove-entity
  [db ent-id]
  (let [ent (impl/entity-at db ent-id)
        layer (remove-back-refs db ent-id (impl/current-layer db))
        retimed-layer (update layer :VAET dissoc ent-id)
        no-ent-layer (assoc retimed-layer :storage (storage/drop-entity (:storage retimed-layer) ent))
        new-layer (reduce (partial remove-entity-from-index ent) no-ent-layer (keys impl/indices))]
    (append-layer db new-layer)))

(defn transact-on-db
  [initial-db ops]
  (loop [[op & rst-ops] ops
         transacted initial-db]
    (if op
      (recur rst-ops (apply (first op) transacted (rest op)))
      (let [new-layer (impl/current-layer transacted)]
        (-> initial-db
            (append-layer new-layer)
            (assoc :present (next-ts initial-db)))))))

(defmacro transact*
  [db exec-fn & ops]
  (when ops
    (loop [[frst-op# & rst-ops#] ops
           res#  [exec-fn db `transact-on-db]
           accum-ops# []]
      (if frst-op#
        (recur rst-ops# res#  (conj  accum-ops#  (vec frst-op#)))
        (list* (conj res#  accum-ops#))))))

(defn- what-if*
  "Operates on the db with the given transactions,
   but without eventually updating it"
  [db f ops]
  (f db ops))

(defmacro what-if [db & ops]
  `(transact* ~db what-if* ~@ops))

(defmacro transact! [db-conn & ops]
  `(transact* ~db-conn swap! ~@ops))

(defn evolution-of
  "The sequence of the values of an entity's attribute, as changed through time"
  [db ent-id attr-name]
  (loop [res (list)
         ts (:present db)]
    (if (neg? ts) ;; -1
      res
      (let [attr (impl/attr-at db ent-id attr-name ts)]
        (recur (conj res {(:current attr)
                          (:value attr)})
               (:previous attr))))))

(defn- layer-before?
  "Returns false is the transaction-timestamp of the
   provided <layer>, is after Instant <t>, true otherwise."
  [t layer]
  (>= 0 (compare (:tx-ts layer) t)))

(defn time-travel
  "Returns the db like it was at layer/time (integer/Instant) <t>.
   If <t> is a positive integer it will include the first t layers,
   otherwise it will include all the layers up to Instant <t>."
  [db t]
  (let [layers (:layers db)
        past-layers (vec
                      (if (and (integer? t)
                               (pos? t))
                        ;; assuming (nth) layer
                        (take (inc t) layers)
                        ;; assuming Instant
                        (take-while (partial layer-before? t) layers)))]
    (if (seq past-layers)
      (impl/->Database past-layers (dec (count past-layers)))
      (throw (IllegalStateException. "No DB subset exists for <t>!")))))