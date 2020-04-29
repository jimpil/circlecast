(ns circlecast.util
  (:import [java.util UUID]
           [java.time Instant]))

(defn uuid! []
  (str (UUID/randomUUID)))

(defn now-instant! []
  (Instant/now))
