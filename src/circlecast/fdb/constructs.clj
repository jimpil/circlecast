(ns circlecast.fdb.constructs
  (:require [circlecast.fdb.storage :as storage]
            [circlecast.util :as ut]
            [jedi-time.core :as jdt]
            [clojure.core.protocols :as p]
            [clojure.datafy :as d])
  (:import [circlecast.fdb.storage InMemory]
           (java.time Instant)
           (java.io Writer)))

(defrecord Database [layers present])
(defrecord Layer    [storage VAET AVET VEAT EAVT ^Instant tx-ts])
(defrecord Entity   [id attrs])
(defrecord Attr     [name value cardinality db-type current previous])

(extend-protocol p/Datafiable
  Database
  (datafy [this]
    (-> (into {} this)
        (update :layers (partial mapv d/datafy))))

  Layer
  (datafy [this]
    (ut/map-vals d/datafy this))

  Entity
  (datafy [this]
    (-> (into {} this)
        (update :attrs (partial ut/map-vals d/datafy))))

  Attr
  (datafy [this]
    (ut/map-vals d/datafy this))
  )

(defonce always (constantly true))

(defn make-index
  "An index is a tree, implemented by nested maps, each level corresponds to either
   entity-id, attribute name or a value, where the leaves of the tree are sets.
   The order of the levels changes from one index to another, to allow different querying
   on different indices. It is possible to reorder a path in the tree to an EAV structure
   using the to-eav function, or transform an EAV to a specific index path using the from-eav,
   both function reside in the metadata of the index. The usage-pred is a predicate to decide
   whether to index a specific attribute in an entity or not."
  ([from-to-eav]
   (make-index {} from-to-eav))
  ([m [from-eav to-eav usage-pred]]
   (with-meta m {:from-eav from-eav
                 :to-eav   to-eav
                 :usage-pred usage-pred})))

(defn from-eav [index]
  (:from-eav (meta index)))

(defn to-eav [index]
  (:to-eav (meta index)))

(defn usage-pred [index]
  (:usage-pred (meta index)))

(defn single? [attr]
  (= :db/single (:cardinality attr)))

(defn ref? [attr]
  (= :db/ref (:db-type attr)))

(defonce indices
  {:VAET [#(vector %3 %2 %1)
          #(vector %3 %2 %1)
          ref?]
   :AVET [#(vector %2 %3 %1)
          #(vector %3 %1 %2)
          always]
   :VEAT [#(vector %3 %1 %2)
          #(vector %2 %3 %1)
          always]
   :EAVT [#(vector %1 %2 %3)
          #(vector %1 %2 %3)
          always]})

(defmethod print-method Layer [this ^Writer w]
  (.write w (str \# (.getName Layer)))
  (-> this
      (update :tx-ts d/datafy)
      (select-keys (keys this))
      (print-method w)))

(defn- decorate-layer
  [layer]
  (reduce-kv
    #(update %1 %2 make-index %3)
    (update layer :tx-ts jdt/undatafy)
    indices))

(def readers
  "The EDN readers required to fully reconstruct an entire DB (from the tagged literals)."
  (merge storage/readers
    {'circlecast.fdb.constructs.Database map->Database
     'circlecast.fdb.constructs.Layer    (comp decorate-layer map->Layer)
     'circlecast.fdb.constructs.Entity   map->Entity
     'circlecast.fdb.constructs.Attr     map->Attr}))

(defn make-db
  "Create an empty database"
  ([]
   (make-db atom))
  ([connection-fn] ;; must return an IAtom
   (connection-fn
     (Database.
       [(Layer.
          (InMemory.) ; storage
          (make-index (:VAET indices)) ; VAET - for graph queries and joins
          (make-index (:AVET indices)) ; AVET - for filtering
          (make-index (:VEAT indices)) ; VEAT - for filtering
          (make-index (:EAVT indices)) ; EAVT - for filtering
          (ut/now-instant!))]
       0))))

(defn entity-at
  "the entity with the given ent-id at the given time (defaults to the latest time)"
  ([db ent-id]
   (entity-at db ent-id (:present db)))
  ([db ent-id nlayer]
   (-> db
       (get-in [:layers nlayer :storage])
       (storage/get-entity ent-id))))

(defn attr-at
  "Returns the attribute of an entity at a given layer - defaults to current one."
  ([^Database db ent-id attr-name]
   (attr-at db ent-id attr-name (:present db)))
  ([db ent-id attr-name nlayer]
   (-> db
       (entity-at ent-id nlayer)
       (get-in [:attrs attr-name]))))

(defn value-of-at
  "Returns the value of a datom at a given time.
   If no layer is provided, defaults to the current (most recent) one."
  ([db ent-id attr-name]
   (-> db (attr-at ent-id attr-name) :value))
  ([db ent-id attr-name nlayer]
   (-> db (attr-at  ent-id attr-name nlayer) :value)))

(defn indx-at
  "Inspecting a specific index at a given time, defaults to current.
   The kind argument may be one of the index name (e.g. :AVET)."
  ([db kind]
   (indx-at db kind (:present db)))
  ([db kind nlayer]
   (-> db
       :layers
       (get nlayer)
       (get kind))))

(defn make-entity
  "Creates an entity. If id is not supplied (recommended),
   a UUID is assigned to the entity."
  ([]
   (make-entity :db/no-id-yet))
  ([id]
   (Entity. id {})))

(defn make-attr
  "creation of an attribute. The name, value and type of an attribute are mandatory arguments, further arguments can be passed as named arguments.
   The type of the attribute may be any keyword or :db/ref, in which case the value is an id of another entity and indexing of backpointing is maintained.
  The named arguments are as follows:
  :cardinality - the cardinality of an attribute, can be either:
                     :db/single - which means that this attribute can be a single value at any given time (this is the default cardinality)
                     :db/multiple - which means that this attribute is actually a set of values. In this case updates of this attribute may be one of the following (NOTE that all these operations accept a set as argument):
                                          :db/add - adds a set of values to the currently existing set of values
                                          :db/reset-to - resets the value of this attribute to be the given set of values
                                          :db/remove - removes the given set of values from the attribute's current set of values"
  ([name value db-type ; these ones are required
    & {:keys [cardinality]
       :or {cardinality :db/single}} ]
   {:pre [(contains? #{:db/single :db/multiple} cardinality)]}
   (Attr. name value cardinality db-type -1 -1)))

(defn add-attr
  "Adds an attribute to an entity."
  [ent attr]
  (let [attr-id (keyword (:name attr))]
    (assoc-in ent [:attrs attr-id] attr)))

(defn current-layer
  "Returns the last Layer in the provided <db>."
  [db]
  (-> db :layers peek))

(def current-storage
  "Returns the :storage of the last Layer in the provided <db>."
  (comp :storage current-layer))
