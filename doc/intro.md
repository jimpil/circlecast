# Introduction to circlecast


## Query
A query is represented as a Clojure map with two mandatory (`:find`, `:where`), and two optional keys (`:join`, `:order-by`).
It will return a sequence of maps with keys per the variables in the `:find` clause (keywordized). 

```clj
{
 :find  [?a ?b]       ;; list/vector/set of variables (determines the returned data-structure)
 :where [[...]]       ;; vector of predeicate-clauses (3-element EAV vector) to be AND-ed - a map is interpreted as a (nested) query 
 :join  [{...}]       ;; vector of queries to join - joining queries can take another 3 keys (:db, :type, :on)
 :order-by [?a :desc] ;; vector of ordering variables with an optional keyword at the end denoting direction)
}
``` 


### Find clause details
As shown above the `:find` clause can be a list/vector/set of variables (symbols starting with `?`). 
The results (maps) returned from a query will be returned in the same type data-structure. 
You should generally avoid sets, unless you can be sure that the returned values are unique, or
you're inside a `^:in?` nested-query (in which case a set is mandatory). Moreover, you should avoid sets
when an `:order-by` clause has been provided. Not only the returned type will NOT be a set, but more 
importantly it doesn't make sense (conflicting logic). Finally, instead of explicit variables you can specify `*`,
in order to include all the variables in the where clause. 


### Where clause details
The `:where` clause can be thought as a sequence of predicates. Each predicate will be compiled from the corresponding 
3-element (EAV) vector. For an entity to be returned it must satisfy ALL predicates. There must be (at least) one common 
variable in all predicate-clauses, which will essentially decide which index to use.

 
#### Nuisances
Restricting on a variable and returning it requires that the restrictive clause comes first. This is technically not a 
problem when restricting by a single value because you could add that value afterwards. However, when restricting with 
anything else (e.g with a set via `^:in?`) you can't do that.

```clj
;; this will work (will return both :currency-id and :currency-a3)
;; but the reverse won't (you will only get :currency-id)
[?currency-id :currency/a3-code ^:in? #{"USD" "EUR}]
[?currency-id :currency/a3-code ?currency-a3]
```


### Join clause details
Join clauses are simply standalone queries supporting 3 extra keys (`:db`, `:type`, `:on`).

- `db`:  the DB to execute the query on (defaults to the DB of the parent query)
- `type`: the type of join to perform (`:inner`,`:left-outer`, `:right-outer`, `full-outer`, `natural`, `cross`) - defaults to `:natural`
- `on`: a vector of 1 or 2 keys to perform the join on (depends on the type of join - natural join doesn't need this for example)  

### Order-by clause details
There is not much to say here. This is a sequence of variables with an optional keyword at the end (`:asc`, `:desc`) 
denoting the order direction (defaults to `:asc`).
 