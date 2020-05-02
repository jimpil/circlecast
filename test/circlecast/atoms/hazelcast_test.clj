(ns circlecast.atoms.hazelcast-test
  (:require [clojure.test :refer :all]
            [circlecast.atoms.hazelcast :as hz])
  (:import (com.hazelcast.core Hazelcast HazelcastInstance)))

(defn- cluster-of [n]
  (repeatedly n #(Hazelcast/newHazelcastInstance)))

(defn- get-atomic-ref
  [ref-name ^HazelcastInstance hz]
  (-> hz
      .getCPSubsystem
      (.getAtomicReference ref-name)))

(deftest hz-atom-test
  (testing "serial updates"
    (let [ref-name "hz-test"
          nodes    (cluster-of 3)
          [atom1 atom2 atom3 :as atoms]
          (->> nodes
               (map (partial get-atomic-ref ref-name))
               (map hz/hz-atom))
          expected (range 100)]

      (println "Initialising with" (reset! atom2 []))

      (doseq [i expected] (swap! (rand-nth atoms) conj i))
      (is (= expected @atom1 @atom2 @atom3))
      (run! #(.shutdown ^HazelcastInstance %) nodes)))

  (testing "parallel updates"
    (let [ref-name "hz-test-parallel"
          nodes    (cluster-of 4)
          [atom1 atom2 atom3 atom4 :as atoms]
          (->> nodes
               (map (partial get-atomic-ref ref-name))
               (map hz/hz-atom))
          _ (println "Initialising with" (reset! atom1 #{}))
          expected (range 200)
          ranges (partition 50 expected)
          loops (doall
                  (map #(future
                          (doseq [i %2]
                            (Thread/sleep (rand-int 100))
                            (swap! %1 conj i))
                          true)
                       atoms
                       ranges))]
      (println "Waiting for nodes to complete their work...")
      (is (every? deref loops)) ;; block the thread until all done
      (is (= (set expected) @atom1 @atom2 @atom3 @atom4))
      (run! #(.shutdown ^HazelcastInstance %) nodes))

    )
  )
