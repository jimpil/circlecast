(ns circlecast.hzatom
  (:import (clojure.lang IAtom IAtom2 IDeref)
           (com.hazelcast.cp IAtomicReference)
           (com.hazelcast.core IFunction)))


(defn- fn->hz-fn
  "Turns a regular clj fn into a hazelcast IFunction."
  ^IFunction [f & args]
  (if (fn? f)
    (reify IFunction
      (apply [_ x]
        (apply f x args)))
    f))

(deftype HazelcastAtom
  [^IAtomicReference aref]

  IDeref
  (deref [_]
    (.get aref))

  IAtom
  (reset [_ nv]
    (.set aref nv)
    nv)

  (swap [_ f]
    (->> (fn->hz-fn f)
         (.alterAndGet aref)))

  (swap [_ f arg1]
    (->> (fn->hz-fn f arg1)
         (.alterAndGet aref)))

  (swap [_ f arg1 arg2]
    (->> (fn->hz-fn f arg1 arg2)
         (.alterAndGet aref)))

  (swap [_ f arg1 arg2 arg-rest]
    (->> (apply fn->hz-fn f arg1 arg2 arg-rest)
         (.alterAndGet aref)))

  (compareAndSet [_ ov nv]
    ;; avoid this method
    (.compareAndSet aref ov nv))

  IAtom2 ;; for completeness - not actually used
  (resetVals [_ nv]
    [(.getAndSet aref nv) nv])

  (swapVals [_ f]
    [(.getAndAlter aref (fn->hz-fn f))
     (.get aref)]) ;; not atomic

  (swapVals [_ f arg1]
    [(.getAndAlter aref (fn->hz-fn f arg1))
     (.get aref)]) ;; not atomic

  (swapVals [_ f arg1 arg2]
    [(.getAndAlter aref (fn->hz-fn f arg1 arg2))
     (.get aref)]) ;; not atomic

  (swapVals [_ f arg1 arg2 arg-rest]
    [(.getAndAlter aref (apply fn->hz-fn f arg1 arg2 arg-rest))
     (.get aref)]) ;; not atomic

  )

(defn hz-atom
  "Given a hazelcast `IAtomicReference` and an optional init-value,
   returns a `HazelcastAtom`. "
  ([hz-atomic-ref]
   (hz-atom hz-atomic-ref nil))
  ([hz-atomic-ref init-val]
   (assert (instance? IAtomicReference hz-atomic-ref)
           "First arg to `hz-atom` MUST be an instance of `com.hazelcast.core.IAtomicReference`.")
   (cond-> (HazelcastAtom. hz-atomic-ref)
           (some? init-val)
           (doto (reset! init-val)))))
