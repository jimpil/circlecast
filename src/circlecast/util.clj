(ns circlecast.util
  (:require [clojure.datafy :as d])
  (:import [java.util UUID]
           [java.time Instant]))

(defn uuid! [] (-> (UUID/randomUUID) str))
(defn now-instant! [] (Instant/now))

(defn find-first
  [pred coll]
  (some #(when (pred %) %) coll))

(defn map-vals
  "Maps <f> across all values of map <m>."
  [f m]
  (persistent!
    (reduce-kv
      (fn [acc k v] (assoc! acc k (f v)))
      (transient {})
      m)))

(defn collify [x]
  (if (coll? x) x [x]))

