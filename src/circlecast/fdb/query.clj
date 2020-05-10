(ns circlecast.fdb.query
  (:require [clojure.set :as CS]
            [circlecast.fdb.constructs :as impl]
            [circlecast.fdb.tables :as tbl])
  (:import (java.util ArrayList)))

(defn variable?
  "A predicate that accepts a string and checks whether it describes a datalog variable (either starts with ? or it is _)"
  ([x] (variable? x true))
  ([x accept_?]  ; intentionally accepts a string and implemented as function and not a macro so we'd be able to use it as a HOF
   (or (and accept_? (= x "_"))
       (= (first x) \?))))

(defmacro clause-term-meta
  "Finds the name of the variable at an item of a datalog clause element. If no variable, returning nil"
  [clause-term]
  (cond
    (list? clause-term)
    (first (filter #(variable? % false)  (map str clause-term))) ; the item is an s-expression, need to treat it as a coll, by going over it and returning the name of the variable
    (variable? (str clause-term) false) (str clause-term) ; the item is a simple variable
    :else nil)) ; the item is a value and not a variable

(declare q)
(defmacro clause-term-expr
  "Create a predicate for each element in the datalog clause"
  [db clause-term]
  (cond
    (variable? (str clause-term)) `impl/always ; simple one, e.g.,  ?a

    (and (set? clause-term) ;; set of constant values (OR semantics)
         (-> clause-term meta :in?)) `(partial contains? ~clause-term)

    (and (set? clause-term) ;; set of constant values (OR semantics)
         (-> clause-term meta :not-in?)) `(complement (partial contains? ~clause-term))


    (and (map? clause-term) ;; nested query returning set (of ids typically)
         (-> clause-term meta :in?)) `#(contains? (q ~db ~clause-term (mapcat vals)) %)

    (and (map? clause-term) ;; nested query returning set (of ids typically)
         (-> clause-term meta :not-in?)) `#(not (contains? (q ~db ~clause-term (mapcat vals)) %))

    (not (list? clause-term))
    `(partial = ~clause-term) ; simple value given, e.g.,  :likes


    (= 2 (count clause-term)) `~(first clause-term) ; an unary predicate, e.g.,  (pos? ?a)
    (variable? (str (second clause-term))) `#(~(first clause-term) % ~(last clause-term)) ;a binary predicate, the variable is the first argument, e.g.,  (> ?a 42)
    (variable? (str (last clause-term))) `#(~(first clause-term) ~(second clause-term) %))) ; a binary predicate, the variable is the second argument, e.g.,  (> ?a 42)

(defmacro  pred-clause
  "Builds a predicate-clause from a query clause (a vector with three elements describing EAV).
   A predicate clause is a vector of predicates that would operate on
    an index, and set for that vector's metadata to be the names of the variables that the user
    assigned for each item in the clause"
  [db clause]
  (loop [[trm# & rst-trm#] clause
           exprs# []
           metas# []]
      (if trm#
        (recur rst-trm#
               (conj exprs# `(clause-term-expr ~db ~trm#))
               (conj metas# `(clause-term-meta ~trm#)))
        (with-meta exprs# {:db/variable metas#}))))

(defmacro q-clauses-to-pred-clauses
  "create a vector of predicate clauses to operate on indices, based on the given vector of clauses"
  [db clauses]
  (loop [[frst# & rst#] clauses
         preds-vecs# []]
    (if frst#
      (recur rst# `(conj ~preds-vecs# (pred-clause ~db ~frst#)))
      preds-vecs#)))

(defn filter-index
  "Function that accepts an index and a path-predicate (which is a tripet of predicates to apply on paths in an index).
   For each path predicates it creates a result path (a triplet representing one path in the index) and returns a seq of result paths."
  [index predicate-clauses]
  (for [ pred-clause predicate-clauses
        :let [[lvl1-prd lvl2-prd lvl3-prd]
              (apply (impl/from-eav index) pred-clause)] ; predicates for the first and second level of the index, also keeping the path to later use its meta
        [k1 l2map] index  ; keys and values of the first level
        :when (try (lvl1-prd k1) (catch Exception _ false))  ; filtering to keep only the keys and the vals of the keys that passed the first level predicate
        [k2  l3-set] l2map  ; keys and values of the second level
        :when (try (lvl2-prd k2) (catch Exception _ false)) ; filtering to keep only the keys and vals of keys that passed the second level predicate
        :let [res (set (filter lvl3-prd l3-set))] ]; keep from the set at the third level only the items that passed the predicate on them
    (with-meta [k1 k2 res] (meta pred-clause)))) ; constructed result clause, while keeping the meta of the query to use it later when extracting variables

(defn items-that-answer-all-conditions
  "takes the sequence of all the items collection, each such collection answered one condition, we test here what are the items that answered all of the conditions
  i.e., what items are found at exactly 'num-of-conditions' of such collections "
  [items-seq num-of-conditions]
  (->> items-seq ; take the items-seq
       (mapcat seq) ; make each collection (actually a set) into a vector
       ;(reduce into []) ;reduce all the vectors into one big vector
       frequencies ; count for each item in how many collections (sets) it was in originally
       (filter #(<= num-of-conditions (val %))) ; keep only the items that answered all of the conditions
       (into #{} (map first)) ; take from the duos the items themselves
       ;(set)
       )) ; return it as set

(defn mask-path-leaf-with-items
  "Returning the path with only the items found in the intersection of that path's items and the relevant items"
  [relevant-items path]
  (update path 2 CS/intersection relevant-items))

(defn combine-path-and-meta
  "This function returns for a (result) path a seq of vectors, each vector is a path from the root of the result path to one of its items, each item
  is followed by its variable name as was inserted in the query (which was kept at the metadata of the (result) path."
  [from-eav-fn path]
  (let [expanded-path [(repeat (first path))
                       (repeat (second path))
                       (peek path)] ; there may be several leaves in each path, so repeating the first and second elements
        meta-of-path (apply from-eav-fn (map repeat (:db/variable (meta path)))) ; re-ordering the path's meta to be in the order of the index
        combined-data-and-meta-path (interleave meta-of-path expanded-path)]
    (apply (partial map vector) combined-data-and-meta-path))) ; returning a seq of vectors, each one is a single result with its meta

(defn bind-variables-to-query
  "A function that receives the query results (result clauses) and transforms each of them into a binding structure.
   A binding structure is a map whose key is a binding pair of an entity-id, and the value is also a map, where its key is a binding pair
   of an attribute, and the value is a binding pair of that found attribute's value. The symbol name in each binding pair is extracted from the tripet's metadata"
  [q-res index]
  (let [seq-res-path (mapcat (partial combine-path-and-meta (impl/from-eav index)) q-res) ; seq-ing a result to hold the meta
        res-path (map #(->> %1 (partition 2) (apply (impl/to-eav index))) seq-res-path)] ; making binding pairs
    (reduce #(assoc-in %1 (butlast %2) (last %2)) {} res-path))) ; structuring the pairs into the wanted binding structure

(defn query-index
  "Querying an index based a seq of predicate clauses. A  predicate clause is composed of 3 predicates, each one to operate on a different level of the index. Querying an index with
  a specific clause-pred returns a result-clause. We then take all the result clauses, find within them the last-level-items that are found in all the result-clauses, and return the result clauses, each contains
  only the last-level-items that are part of all the result-clauses."
  [index pred-clauses]
  (let [result-clauses (filter-index index pred-clauses) ; the predicate clauses from the root of the index to the leaves (a leaf of an index is a set)
        relevant-items (items-that-answer-all-conditions (map peek result-clauses) (count pred-clauses)) ; the set of elements, each answers all the pred-clauses
        cleaned-result-clauses (map (partial mask-path-leaf-with-items relevant-items) result-clauses)] ; the result clauses, now their leaves are filtered to have only the items that fulfilled the predicates
    (filter (comp seq peek) cleaned-result-clauses))) ; of these, we'll build a subset of the index that contains the clauses with the leaves (sets), and these leaves contain only the valid items

(defn single-index-query-plan
  "A query plan that is based on querying a single index"
  [query indx db]
  (let [index (impl/indx-at db indx)
        q-res (query-index index query)]
    (bind-variables-to-query q-res index)))

(defn index-of-joining-variable
  "A joining variable is the variable that is found on all of the query clauses"
  [query-clauses]
  (let [metas-seq  (map #(:db/variable (meta %)) query-clauses) ; all the metas (which are vectors) for the query
        collapsing-fn (fn [accV v] (map #(when (= %1 %2) %1) accV v)) ; going over the vectors, collapsing each onto another, term by term, keeping a term only if the two terms are equal
        collapsed (reduce collapsing-fn metas-seq)] ; using the above fn on the metas, eventually get a seq with one item who is not null, this is the joining variable
    (first (keep-indexed #(when (variable? %2 false) %1)  collapsed)))) ; returning the index of the first element that is a variable (there's only one)

(defn build-query-plan
  "Upon receiving a database and query clauses, this function responsible to deduce on which index in the db it is best to perform the query clauses,
   and then return a query plan, which is a function that accepts a database and executes the plan on it."
  [query]
  (let [term-ind (index-of-joining-variable query)
        ind-to-use (case (int term-ind) 0 :AVET 1 :VEAT 2 :EAVT)]
    (partial single-index-query-plan query ind-to-use)))

(defn resultify-bind-pair
  "A bind pair is composed of two elements - the variable name and its value.
   Resultifying means to check whether the variable is suppose to be part of the
   result, and if it does, adds it to the accumulated result."
  [vars-set accum pair]
  (let [[var-name v] pair]
    (cond-> accum
            (vars-set var-name)
            (assoc (keyword (subs var-name 1)) v))))

(defn resultify-av-pair
  "An av pair is a pair composed of two binding pairs, one for an attribute and one for the attribute's value"
  [vars-set accum-res av-pair]
  (reduce (partial resultify-bind-pair vars-set) accum-res av-pair))

(defn locate-vars-in-query-res
  "this function would look for all the bindings found in the query result and return the binding that were requested by the user (captured at the vars-set)"
  [vars-set q-res]
  (let [[e-pair av-map]  q-res
        e-res (resultify-bind-pair vars-set {} e-pair)]
    (map (partial resultify-av-pair vars-set e-res) av-map)))

(defn unify
  "Unifying the binded query results with variables to report"
  [binded-res-col needed-vars]
  (eduction
    (map (partial locate-vars-in-query-res needed-vars))
    (map (partial apply merge))
    binded-res-col))

(defmacro symbol-col-to-set
  [find-clause where-clause]
  (if (= '* (first find-clause))
    ;; set of all vars appearing in the :where clause
    (let [all-vars (volatile! #{})]
      (clojure.walk/postwalk
        (fn [x]
          (when (symbol? x)
            (let [sx (str x)]
              (when (variable? sx false)
                (vswap! all-vars conj sx))))
          x)
        where-clause)
      @all-vars)
    ;; ;; set of all vars provided in the :find clause
    (set (map str find-clause))))

(defmacro q*
  [db query]
  `(let [pred-clauses# (q-clauses-to-pred-clauses ~db ~(:where query)) ; transforming the clauses of the query to an internal representation structure called query-clauses
         needed-vars#  (symbol-col-to-set ~(:find query) '~(:where query))  ; extracting from the query the variables that needs to be reported out as a set
         query-plan#   (build-query-plan pred-clauses#) ; extracting a query plan based on the query-clauses
         query-internal-res# (query-plan# ~db)] ;executing the plan on the database
     (unify query-internal-res# needed-vars#))) ;unifying the query result with the needed variables to report out what the user asked for


(defn xf-sort-by
  "A sorting transducer. Mostly a syntactic improvement to allow composition of
  sorting with the standard transducers, but also provides a slight performance
  increase over transducing, sorting, and then continuing to transduce."
  ([kfn]
   (xf-sort-by kfn compare))
  ([kfn cmp]
   (fn [rf]
     (let [temp-list (ArrayList.)]
       (fn
         ([]
          (rf))
         ([xs]
          (reduce rf xs (sort-by kfn cmp (.toArray temp-list))))
         ([xs x]
          (.add temp-list x)
          xs))))))

(defmacro realise*
  [DB query xform order-by container]
  (let [into-container (if (seq? container) `sequence `(partial into ~container))] ;; list/set/vector
    `(~into-container
       (cond-> ~xform
               ~order-by
               (comp (xf-sort-by
                       (apply juxt (second ~order-by))
                       (first ~order-by))))
       (q* ~DB ~query))))

(defonce directions #{:asc :desc})

(defmacro order-by
  [order-keys]
  `(when-let [ks# (not-empty ~order-keys)]
     (let [direction# (directions (peek ks#))
           descending?# (= :desc direction#)]
       [(if descending?# #(compare %2 %1) compare)
        (cond->> ks#
                 descending?# pop
                 true (map (comp keyword #(subs % 1) str))
                 true vec)])))

(defmacro joining*
  [db joining-clauses]
  (vec
    (for [clause joining-clauses]
      `(assoc '~clause :jrs
         (realise* ~(:db clause db)
                   ~(:from clause)
                   (map identity)
                   (order-by ~(:order-by clause))
                   (empty ~(:find clause)))))))

(defn do-join
  [left {:keys [style on jrs]
         :or {style :natural}}]
  (case style
      :natural     (tbl/natural-join left jrs)
      :cross       (tbl/cross-join   left jrs)
      :inner       (apply tbl/inner-join left jrs on)
      :left-outer  (apply tbl/left-outer-join  left jrs on)
      :right-outer (apply tbl/right-outer-join left jrs on)
      :full-outer  (apply tbl/full-outer-join  left jrs on)))

;; public API

(defmacro q
  "querying the database using datalog queries built in a map structure
  ({:find [variables*] :where [ [e a v]* ]}). (after the where there are clauses)
  At the moment support only filtering queries, no joins is also assumed."
  ([db query]
   `(q ~db ~query nil))
  ([db query xform]
   `(let [DB# ~db
          joins# '~(:join query)
          no-joins?# (empty? joins#)
          container# ~(empty (:find query))
          ;_# (println joins#)
          ret# (realise* DB#
                        ~query
                        (or ~xform (map identity))
                        (when no-joins?#
                          ~(order-by (:order-by query)))
                        container#)]
      (if no-joins?#
        ret#
        (let [[cmp# ks#] ~(order-by (:order-by query))]
          (cond->> (reduce do-join ret# (joining* DB# ~(:join query)))
                   ks# (sort-by (apply juxt ks#) cmp#)))))))

(defmacro q-all
  "Executes the same query on multiple <dbs>.
   Returns a list of result-sets (per the provided
   <query>), in the same order as <dbs>."
  ([dbs query]
   `(for [db# ~dbs] (q db# ~query)))
  ([dbs query xform]
   `(for [db# ~dbs] (q db# ~query ~xform))))

(defmacro with-query
  "Helper macro for avoiding having to pass queries as
   compile-time constants. Takes a <query> "
  [query [op db _ xform]]
  `(~op ~db ~query ~xform))

;; Queries as Vars
(defmacro qv
  "Similar to `q` but expects the <query> as a Var."
  ([db query]
   `(do
      (assert (var? ~query) "`qv` expects the query as a Var!")
      (with-query ~(var-get (eval query)) (q ~db nil nil))))
  ([db query xform]
   `(do
      (assert (var? ~query) "`qv` expects the query as a Var!")
      (with-query ~(var-get (eval query)) (q ~db nil ~xform)))))

(defmacro qv-all
  "Similar to `q-all` but expects the <query> as a Var."
  ([db query]
   `(do
      (assert (var? ~query) "`qv-all` expects the query as a Var!")
      (with-query ~(var-get (eval query)) (q-all ~db nil nil))))
  ([db query xform]
   `(do
      (assert (var? ~query) "`qv-all` expects the query as a Var!")
      (with-query ~(var-get (eval query)) (q-all ~db nil ~xform)))))

;; Queries as functions
(defmacro qf
  "Similar to `q` but expects the <query> as a no-arg function."
  ([db query]
   `(do
      (assert (fn? ~query) "`qf` expects the query as a function!")
      (with-query ~((eval query)) (q ~db nil nil))))
  ([db query xform]
   `(do
      (assert (fn? ~query) "`qf` expects the query as a function!")
      (with-query ~((eval query)) (q ~db nil ~xform)))))

(defmacro qf-all
  "Similar to `q-all` but expects the <query> as a no-arg function."
  ([db query]
   `(do
      (assert (fn? ~query) "`qf-all` expects the query as a function!")
      (with-query ~((eval query)) (q-all ~db nil nil))))
  ([db query xform]
   `(do
      (assert (fn? ~query) "`qf-all` expects the query as a function!")
      (with-query ~((eval query)) (q-all ~db nil ~xform)))))


