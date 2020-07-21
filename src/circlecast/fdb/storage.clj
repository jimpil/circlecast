(ns circlecast.fdb.storage
  (:require [clojure.core.protocols :as p]))

(defprotocol Storage
  (get-entity   [storage e-id] )
  (write-entity [storage entity])
  (drop-entity  [storage entity]))

(defrecord InMemory []
  Storage
  (get-entity [storage e-id]
    (e-id storage))
  (write-entity [storage entity]
    (assoc storage (:id entity) entity))
  (drop-entity [storage entity]
    (dissoc storage (:id entity))))

(def readers
  {'circlecast.fdb.storage.InMemory  map->InMemory})

(extend-protocol p/Datafiable
  InMemory
  (datafy [this]
    (into {} this))
  )
