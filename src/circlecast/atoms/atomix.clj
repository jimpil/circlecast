(ns circlecast.atoms.atomix
  (:import (clojure.lang IDeref IAtom IAtom2)))

(import '(io.atomix.core.value AtomicValue))

;; The API offered by atomix is inferior to the one offered by hazelcast.
;; As you see below, `swap!` must be implemented on top of `compare-and-set!`,
;; which means that two IO operations (at least) are needed. This is because
;; there is no notion of a function in atomix. Moreover, even though the notion
;; of listeners does exist, it cannot be integrated with watches as the latter
;; are referred to by keys (as opposed to `AtomicValueListener` objects).
;; As a result, I'm reluctant to recommend this atom. I'm hoping that the devs
;; realise this shortcoming - in fact I may drop an issue on github.
;; https://github.com/atomix/atomix/issues/1072


(defn- swapx!
  [atomix-atom f & args]
  (loop [] ;; this is the best we can do for now :(
    (let [ov (deref atomix-atom)
          nv (apply f ov args)]
      (if (compare-and-set! atomix-atom ov nv)
        [ov nv]
        (recur)))))


(deftype AtomixAtom
  [^AtomicValue aref]

  IDeref
  (deref [_]
    (.get aref))

  IAtom
  (reset [_ nv]
    (.set aref nv)
    nv)

  (swap [this f]
    (second (swapx! this f)))

  (swap [this f arg1]
    (second (swapx! this f arg1)))

  (swap [this f arg1 arg2]
    (second (swapx! this f arg1 arg2)))

  (swap [this f arg1 arg2 arg-rest]
    (second (apply swapx! this f (list* arg1 arg2 arg-rest))))

  (compareAndSet [_ ov nv]
    (.compareAndSet aref ov nv))

  IAtom2 ;; for completeness - not actually used
  (resetVals [_ nv]
    [(.getAndSet aref nv) nv])

  (swapVals [this f]
    (swapx! this f))

  (swapVals [this f arg1]
    (swapx! this f arg1))

  (swapVals [this f arg1 arg2]
    (swapx! this f arg1 arg2))

  (swapVals [this f arg1 arg2 arg-rest]
    (apply swapx! this f (list* arg1 arg2 arg-rest)))

  )