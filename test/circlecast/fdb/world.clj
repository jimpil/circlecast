(ns circlecast.fdb.world
  (:require [clojure.test :refer :all]
            [circlecast.fdb.constructs :as impl]
            [clojure.string :as str]
            [circlecast.data.countries :as countries]
            [circlecast.fdb.manage :as M]
            [circlecast.fdb.query :as Q]
            [circlecast.fdb.core :as core]
            [circlecast.util :as ut])
  (:import (java.text Normalizer Normalizer$Form)))


(defn make-countries
  [country-data country-name->currency-id]
  (for [[a2-code v] country-data]
    (let [country-name (:name v)
          currency-id (country-name->currency-id (str/upper-case country-name))]
      (-> (impl/make-entity)
        (impl/add-attr (impl/make-attr :country/name           country-name      :string))
        (impl/add-attr (impl/make-attr :country/capital        (:capital v)      :string))
        (impl/add-attr (impl/make-attr :country/continent-code (:continent-a2 v) :string))
        (impl/add-attr (impl/make-attr :country/phone-code     (:phone-code v)   :string))
        (impl/add-attr (impl/make-attr :country/a3-code        (:a3-code v)      :string))
        (impl/add-attr (impl/make-attr :country/a2-code        a2-code           :string))
        (impl/add-attr (impl/make-attr :country/currency       currency-id       :db/ref))
        ))
    )
  )

(defn normalise [^String s]
  (let [norm (Normalizer/normalize s Normalizer$Form/NFD)]
    (cond-> norm
            (not= norm s)
            (str/replace "\\p{M}" ""))))

(defn make-currencies
  [data]
  (for [currency data :when (empty? (get currency "Withdrawal Date"))]
    (-> (impl/make-entity)
        (impl/add-attr (impl/make-attr :currency/entity (normalise (get currency "Entity")) :string))
        (impl/add-attr (impl/make-attr :currency/name (get currency "Currency") :string))
        (impl/add-attr (impl/make-attr :currency/a3-code (get currency "Alphabetic Code") :string))
        (impl/add-attr (impl/make-attr :currency/num-code (get currency "Numeric Code") :string))
        (impl/add-attr (impl/make-attr :currency/minor-units (try (Long/parseLong (get currency "Minor unit"))
                                                                  (catch NumberFormatException _ 0)) :number))
        #_(impl/add-attr (impl/make-attr :currency/withdrawn? (boolean (not-empty (get currency "Withdrawal Date"))) :boolean)))
    )
  )



(def world-db (M/get-db-conn "world"))

(defonce setup-world!
  (memoize
    (fn []
      (core/transact! world-db (core/add-entities (make-currencies @countries/currencies)))
      (core/transact! world-db (core/add-entities
                                 (make-countries (countries/country-data)
                                                 (->> @world-db
                                                      impl/current-storage
                                                      (map (juxt (comp :value :currency/entity :attrs val) key))
                                                      (into {})))))


      :done)))

(deftest query-tests

  (setup-world!)

  (testing "find all country names and their capitals - ordered vector of maps"
    (let [ret (Q/q @world-db
                   {:find  [?country-name ?capital]
                    :where [[?e :country/name ?country-name]
                            [?e :country/capital ?capital]]
                    :order-by [?capital :desc]})]

      (is (= 250 (count ret)))
      (is (vector? ret))
      (is (every? map? ret))
      (is (every? some? (map :country-name ret)))
      (is (every? some? (map :capital ret)))))

  (testing "find all country names - set of raw values"
    (let [ret (Q/q @world-db
                   {:find #{?country-name}
                    :where [[?e :country/name ?country-name]]}
                   (map :country-name))]

      (is (= 250 (count ret)))
      (is (set? ret))
      (is (every? string? ret))))


  (testing "find all capital names that start with 'A' - sequence of 7 raw values"
    (let [ret (Q/q @world-db
                   {:find (?capital)
                    :where [[?e :country/capital (str/starts-with? ?capital "A")]]
                    :order-by [?capital]}
                   (comp (take 7)
                         (map :capital)))]

      (is (= 7 (count ret)))
      (is (seq? ret))
      (is (every? #(str/starts-with? % "A") ret))))

  (testing "find all country names whose currency is EUR (via nested query) - sequence of maps"
    (let [ret (Q/q @world-db
                   {:find (?country-name)
                    :where [[?e :country/name ?country-name]
                            [?e :country/currency
                             ^:in? {:find  #{?currency-id}
                                    :where [[?currency-id :currency/a3-code "EUR"]]}]]})]

      (is (= 27 (count ret)))
      (is (seq? ret))
      (is (every? map? ret))))

  (testing "find all country names whose currency is EUR (via implicit JOIN) - sequence of maps"
    (let [param "EUR"
          ret (Q/q @world-db
                   {:find (?country-name)
                    :params [$A3]
                    :where [[?e :country/name ?country-name]
                            [?e :country/currency ?currency-id]
                            [?currency-id :currency/a3-code $A3]]}
                   nil
                   param)]

      (is (= 27 (count ret)))
      (is (seq? ret))
      (is (every? map? ret))))


  (testing "find the capital names and minor-units of all countries whose currency is EUR/OMR (via inner join) - vector of maps"
    (let [ret (Q/q @world-db
                   {:find [?capital ?minor-units]
                    :where [[?e :country/capital ?capital]
                            [?e :country/currency ?currency-id]
                            [?currency-id :currency/a3-code ^:in? #{"EUR" "OMR"}]
                            [?currency-id :currency/minor-units ?minor-units]
                            ]
                    })]

      (is (= 28 (count ret)))
      (is (vector? ret))
      (is (every? map? ret))
      )
    )


  (testing "find the capital names and a3-code/minor-units of all countries whose currency is EUR/OMR (via inner join) in descending order - seq of maps"
    (let [ret (Q/q @world-db
                   {:find [?capital ?minor-units ?currency-a3]
                    :where [[?e :country/capital ?capital]
                            [?e :country/currency ?currency-id]
                            [?currency-id :currency/a3-code ^:in? #{"EUR" "OMR"}]
                            ;; binding a restrive variable for return MUST
                            ;; come after the restrictive clause
                            [?currency-id :currency/a3-code     ?currency-a3]
                            [?currency-id :currency/minor-units ?minor-units]
                            ]
                    :order-by [?minor-units ?capital :desc]
                    })]

      (is (= 28 (count ret)))
      (is (sequential? ret)) ;; always expect seq for ordered results
      (is (every? map? ret))
      (is (= ["Muscat" "OMR" 3] ((juxt :capital :currency-a3 :minor-units) (first ret))))
      )
    )



  )

(comment

  ;; world setup

  (require '[clj-memory-meter.core :as mm])
  (mm/measure @world-db) ;; => "2.6 MB"


  )
