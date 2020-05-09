(ns circlecast.util
  (:require [clojure.datafy :as d])
  (:import [java.util UUID HashMap]
           [java.time Instant]))

(defn uuid! []
  (-> (UUID/randomUUID) str))

(defn now-instant! []
  (-> (Instant/now) d/datafy))

(defn find-first
  [pred coll]
  (some
    #(when (pred %) %)
    coll))

(defn map-vals
  [f coll]
  (persistent!
    (reduce-kv
      (fn [acc k v] (assoc! acc k (f v)))
      (transient {})
      coll)))

(defn into-container-fn
  [coll]
  (if (seq? coll)
    sequence
    (partial into (empty coll))))
