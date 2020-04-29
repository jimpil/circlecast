(ns circlecast.fdb.manage
  "Management of db connections (from the user's perspective), internally creates / drops dbs."
  (:require [circlecast.fdb.constructs :as impl]))

(def ^:private __ALL-DBS__ (atom {}))

(defn- put-db
  [dbs db-name make-db!]
  (if (contains? dbs db-name)
    dbs
    (assoc dbs db-name (make-db!))))

(defn- drop-db
  [dbs db-name]
  (dissoc dbs db-name))

(defn get-db-conn
  ([db-name]
   (get-db-conn impl/make-db db-name))
  ([make-db! db-name]
   (let [stored-db-name (keyword db-name)]
     (stored-db-name (swap! __ALL-DBS__ put-db stored-db-name make-db!)))))

(defn drop-db-conn
  [db-name]
  (let [stored-db-name (keyword db-name)]
    (swap! __ALL-DBS__ drop-db stored-db-name)
    nil))

(defn db-from-conn [conn] @conn)

