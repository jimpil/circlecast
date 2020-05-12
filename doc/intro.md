# Introduction to circlecast


## Query
A query is represented as a Clojure map with two mandatory (`:find`, `:where`), and two optional keys (`:params`, `:order-by`).
It will return a sequence of maps with keys per the variables in the `:find` clause (keywordized). 

```clj
{
 :find   [?a ?b]      ;; list/vector/set of variables (determines the returned data-structure)
 :params [$x $y]      ;; sequential of parameters arriving from the outside world
 :where  [[...]]      ;; sequential of predicate-clauses (3-element EAV vector) to be AND-ed - a map is interpreted as a (nested) query 
 :order-by [?a :desc] ;; sequential of ordering variables with an optional keyword at the end denoting direction)
}
``` 

### Find clause details
As shown above the `:find` clause can be a list/vector/set of variables (symbols starting with `?`). 
The results (maps) returned from a query will be collected in the same type data-structure. 
You should generally avoid sets, unless you can be sure that the returned values are unique, or
you're inside a `^:in?`/`^:not-in?` nested-query (in which case a set container is actually mandatory). 
Moreover, you should avoid sets when an `:order-by` clause has been provided. Not only the returned type will NOT be a set, 
but more importantly it doesn't make sense (conflicting logic). Finally, instead of explicit variables you can specify `*`,
in order to include all the variables found in the `:where` clause. 


### Where clause details
The `:where` clause can be thought as a sequence of predicates. Each predicate will be compiled from the corresponding 
3-element (EAV) vector. Implicit (natural) joins are supported, which is to say that more than one index can be queried.
This removes the need for having a common variable in all predicate-clauses (a restriction of the original implementation).

 
#### Nuisances
Optimising the query falls on the caller. In other words, different order of :`where` clauses will result in different query plans.
Therefore it's always a good idea to order them in descending restrictive order, and additionally, group them by the index they address.
Finally, restricting on a variable **and** returning it requires that the restrictive clause comes first. 
This is technically not a problem when restricting by a single value because you could add that value afterwards. 
However, when restricting with anything else (e.g with a set via `^:in?`) you can't do that.

```clj
;; this will work (will return both :currency-id and :currency-a3)
;; but the reverse won't (you will only get :currency-id)

[?currency-id :currency/a3-code ^:in? #{"USD" "EUR}]
[?currency-id :currency/a3-code ?currency-a3]
```

### Order-by clause details
There is not much to say here. This is a sequence of variables with an optional keyword at the end (`:asc`, `:desc`) 
denoting the order direction (defaults to `:asc`). There is a small penalty that comes with ordering, especially for large datasets. 
That said, it should still be faster than manually sorting the final result, regardless of whether that is lazy or eager.

### Limiting
Limiting the number of results returned is not part of the query language. 
Instead, the caller is free to pass a `(take n)` transducer as an argument to `Q/q`.

```clj
(Q/q @world-db
     {:find (?capital)
      :where [[?e :country/capital (str/starts-with? ?capital "A")]]
      :order-by [?capital]}
     (comp (take 5)        ;; <<=======
           (map :capital)))

=> ("Abu Dhabi" "Abuja" "Accra" "Adamstown" "Addis Ababa")
```


### Peculiarities 
Even though queries are pure data, the API exposed is macro-based. This means that queries (second argument to `q`) must be compile-time constants.
If you want to define the actual queries (the maps) in a separate place than the querying itself, there is a helper macro `with-query`, 
and q-variants (`qv` for vars, `qs` for symbols, `qf` for functions) that build upon it. 
For example:

```clj
(def all-country-names-query  ;; define a Var
  '{:find #{?country-name}
    :where [[?e :country/name ?country-name]]})

(Q/q    @world-db #'all-country-names-query) ;; will NOT work
(Q/qv   @world-db #'all-country-names-query) ;; will work

(defn minor-units-of [& a3-codes] ;; define a function
  (let [code-set (with-meta (set a3-codes) {:in? true})] ;; <<====
    {:find  '[?currency-id ?minor-units ?currency-a3]
     :where [['?currency-id :currency/a3-code     code-set] ;; ^:in code-set
             '[?currency-id :currency/a3-code     ?currency-a3]
             '[?currency-id :currency/minor-units ?minor-units]]}))

(Q/q    @world-db (partial minor-units-of "EUR" "USD")) ;; will NOT work
(Q/qf   @world-db (partial minor-units-of "EUR" "USD")) ;; will work

```
As seen above, because of the macro-based API, treating queries as higher order, requires a bit of massaging.
Most notably, you have to make sure the set(s) that act as predicates must have the right `:in?` metadata.
Also instead of quoting the whole map (which was convenient in the first case), notice how in the second case 
individual vectors or symbols are quoted. That's because we want to avoid quoting the `code-set` local var. 
You can build your own q-variants quite easily.

### Across databases

Querying more than one DB using the same query is trivial. All we need to do is to call `q` on those multiple DBs 
(see `q-all`) and concatenate the results.

