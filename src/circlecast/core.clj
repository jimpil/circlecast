(ns circlecast.core
  (:require [circlecast.fdb
             [constructs :as impl]
             [operations :as ops]]))

;; CONNECTION MANAGEMENT
(defonce DB-CONNECTIONS (atom {}))

(defn- put-db
  [dbs db-name make-db!]
  (if (contains? dbs db-name)
    dbs
    (assoc dbs db-name (make-db!))))

(defn- drop-db
  [dbs db-name]
  (dissoc dbs db-name))

(defn get-db-conn
  "Returns the DB connection named <db-name>.
   If it doesn't exist, it is created and remembered."
  ([db-name]
   (get-db-conn impl/make-db db-name))
  ([make-db! db-name]
   (let [stored-db-name (keyword db-name)]
     (stored-db-name
       (swap! DB-CONNECTIONS put-db stored-db-name make-db!)))))

(defn drop-db-conn
  [db-name]
  (let [stored-db-name (keyword db-name)]
    (swap! DB-CONNECTIONS drop-db stored-db-name)
    nil))

;;================================================
;; TRANSACTION MACROS

(defn- what-if*
  "Operates on the db with the given transactions,
   without actually updating it."
  [db f ops]
  (f db ops))

(defn evolution-of
  "The sequence of the values of an entity's attribute,
   as changed through time."
  [db ent-id attr-name]
  (loop [res (list)
         ts (:present db)]
    (if (neg? ts) ;; -1
      res
      (let [attr (impl/attr-at db ent-id attr-name ts)]
        (recur (conj res {(:current attr)
                          (:value attr)})
               (:previous attr))))))

(defmacro what-if
  "Similar to `transact`, but doesn't actually update the db."
  [db-conn & ops]
  `(ops/transact* (deref ~db-conn) what-if* ~@ops))

(defmacro transact!
  "Updates the db according to the provided <ops>.
   Supported operations are invocations of public functions
   from `circlecast.fdb.operations` (e.g. add-entity, update-entity, remove-entity etc)."
  [db-conn & ops]
  `(ops/transact* ~db-conn swap! ~@ops))

;;======================================
;; TIME TRAVEL

(defn- layer-before?
  "Returns false is the transaction-timestamp of the
   provided <layer>, is after Instant <t>, true otherwise."
  [t layer]
  (>= 0 (compare (:tx-ts layer) t)))

(defn db-at
  "Returns the db like it was at layer/time (integer/Instant) <t>.
   If <t> is a positive integer it will include the first t layers,
   otherwise it will include all the layers up to Instant <t> (inclusive)."
  [db t]
  (let [layers (:layers db)
        past-layers (vec
                      (if (and (integer? t)
                               (pos? t))
                        ;; assuming (nth) layer
                        (take t layers)
                        ;; assuming Instant
                        (take-while (partial layer-before? t) layers)))]
    (impl/->Database past-layers (count past-layers))))

