# circlecast




## What 

[CircleDB](https://www.aosabook.org/en/500L/an-archaeology-inspired-database.html) 
on top of [Hazelcast](https://hazelcast.com/) (see `com.hazelcast.cp.IAtomicReference` for the exact construct utilised).

####TL;DR
An _in-memory, index-optimized, query-supporting, library developer-friendly, time-aware, functional (immutable) 
database_, now distributed via Hazelcast (for persistence keep reading).


## DB structure
One has to really read the article linked in the first sentence of this README, in order to fully understand/appreciate
the design around this. In short, a database is essentially a collection of layers. Each layer represents a point in time
where a new piece of information was found. That includes insertions, updates and deletions (i.e. always growing by adding layers).
The last layer represents the latest information (for all the entities). An entity is something uniquely named with a set of attributes, 
each having a value (at a particular point in time). Rather datomic-like actually...


## DB connection
Immutable databases need a way of transitioning from one state to another. For dev/test work, a regular Clojure atom will suffice.
For a distributed atom, refer to `circlecast.hzatom.HazelcastAtom` and its constructor-fn `circlecast.hzatom.hz-atom.`


## Usage 

Assuming a Hazelcast instance `hz-instance` (e.g. the result of `(Hazelcast/newHazelcastInstance)`), the following returns an empty DB.

```clj
(require '[circlecast.hzatom :refer [hz-atom]]
         '[circlecast.fdb.constructs :refer [make-db]])

(def db-name "myDB")
;; make-db can be called w/o args but returns  regular atom
(def DB (make-db (partial hz-atom (-> hz-instance .getCPSubsystem (.getAtomicReference db-name)))))

@DB 
;; => an empty DB
#circlecast.fdb.constructs.Database{:layers 
  [#circlecast.fdb.constructs.Layer{:storage #circlecast.fdb.storage.InMemory{},
                                    :VAET {},
                                    :AVET {},
                                    :VEAT {},
                                    :EAVT {},
                                    :instant #object[java.time.Instant 
                                                     0x53327a7f
                                                     "2020-04-29T15:53:16.282574Z"]}],
  :top-id "0",
  :curr-time 0}

```
See `circlecast.fdb.hospital.clj` for a more involved example.

## `duratom` integration
Here is how to create an atom distributed via Hazelcast, and persisted on PostgresDB via [duratom](https://github.com/jimpil/duratom):

```clj
(require '[circlecast.hzatom :refer [hz-atom]]
         '[circlecast.fdb.constructs :refer [make-db]
         '[duratom.core :refer [with-atom-ctor duratom]]])

(def db-name "myPersistedDB")
(def DB 
  (with-atom-ctor (partial hz-atom (-> hz-instance .getCPSubsystem (.getAtomicReference db-name)))
    (duratom :postgres-db
             :db-config "any db-spec as understood by clojure.java.jdbc"
             :table-name "my_table"
             :row-id 0
             :init (make-db identity)))) ;; init-value is only relevant when nothing is found in storage 

@DB 
;; => an empty DB (same as before)
```


## Persistence
How data should be persisted in this model (distributed memory grid), will ultimately depend on the actual application. 
For example, if we accept the fact that the data is duplicated across the cluster (and therefore unlikely to be lost at any given time),
a simple _periodic-backup_ approach could potentially suffice (see `hazelcast-client` for talking to a cluster when not a member of it). 
Since the entire DB is an EDN value, this approach would be somewhat trivial to implement. 

If a more formal/consistent approach is required, Hazelcast itself provides enterprise storage [solutions](https://hazelcast.com/product-features/imdg-comparison/). 

Putting that stuff aside for a moment, I am more interested/excited about how nicely this plays with [duratom](https://github.com/jimpil/duratom),
which is wrapper-type for atoms whose whole purpose is to add durability (on every state change). Why is that relevant, I hear you ask...
Well, this entire library is based around the `circlecast.hzatom.HazelcastAtom` type (implementing `IAtom`, `IAtom2` \& `IDeref`), 
which a `duratom` will happily wrap. This is a win-win situation! Not only one can add persistence to a hazelcast atom 
(by wrapping it with a `duratom`), but existing `duratom` users can also add distribution to their atoms (by replacing them with hazelcast atoms).
Frankly, I never intended for `duratom` to be something distributable, but in this context (where atomicity **and** distribution are provided by the underlying atom) 
everything falls into place quite beautifully.  
 


## License

Copyright © 2020 Dimitrios Piliouras

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
