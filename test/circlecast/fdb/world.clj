(ns circlecast.fdb.world
  (:require [clojure.test :refer :all]
            [circlecast.fdb.constructs :as impl]
            [clojure.string :as str]
            [circlecast.data.countries :as countries]
            [circlecast.fdb.manage :as M]
            [circlecast.fdb.query :as Q]
            [circlecast.fdb.core :as core])
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


(defn make-currencies
  [data]
  (for [currency data :when (empty? (get currency "Withdrawal Date"))]
    (-> (impl/make-entity)
        (impl/add-attr (impl/make-attr :currency/entity (get currency "Entity") :string))
        (impl/add-attr (impl/make-attr :currency/name (get currency "Currency") :string))
        (impl/add-attr (impl/make-attr :currency/a3-code (get currency "Alphabetic Code") :string))
        (impl/add-attr (impl/make-attr :currency/num-code (get currency "Numeric Code") :string))
        (impl/add-attr (impl/make-attr :currency/minor-units (try (Long/parseLong (get currency "Minor unit"))
                                                                  (catch NumberFormatException _ 0)) :number))
        #_(impl/add-attr (impl/make-attr :currency/withdrawn? (boolean (not-empty (get currency "Withdrawal Date"))) :boolean)))
    )
  )

(defn normalise [^String s]
  (let [norm (Normalizer/normalize s Normalizer$Form/NFD)]
    (cond-> norm
            (not= norm s)
            (str/replace "\\p{M}" ""))))

(def world-db (M/get-db-conn "world"))

(comment

  ;; world setup
  (core/transact! world-db
                  (core/add-entities (make-currencies @countries/currencies)))

  (let [curr-name->curr-id  (->> @world-db
                                 impl/last-layer-storage
                                 (map (juxt (comp :value :currency/entity :attrs val) key))
                                 (into {}))]
    (core/transact! world-db (core/add-entities
                               (make-countries (countries/country-data) curr-name->curr-id))))

  (require '[clj-memory-meter.core :as mm])
  (mm/measure @world-db) ;; => "2.6 MB"

  ()

  (Q/q @world-db
       {:find (?name ?capital)
        :where [[?e :country/name    ?name]
                [?e :country/capital (str/starts-with? ?capital "A")]
                [?e :country/currency ^:in?
                 {:find  #{?currency-id}
                  :where [[?currency-id :currency/a3-code ^:in? #{"EUR" "USD"}]]}]

                ]
        :order-by [?capital :desc]

        }
       ;(map :country-name)
       )




  )
