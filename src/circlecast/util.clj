(ns circlecast.util
  (:require [clojure.datafy :as d])
  (:import [java.util UUID]
           [java.time Instant]))

(defn uuid! []
  (-> (UUID/randomUUID) str))

(defn now-instant! []
  (-> (Instant/now) d/datafy))
