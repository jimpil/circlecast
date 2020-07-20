(ns circlecast.data.countries
  (:require [clojure.data.json :as json]))

(defonce country-a2->continent-a2
  (-> "http://country.io/continent.json"
      slurp
      json/read-str
      delay))

(defonce country-a2->country-name
  (-> "http://country.io/names.json"
      slurp
      json/read-str
      delay))

(defonce country-a2->capital
  (-> "http://country.io/capital.json"
      slurp
      json/read-str
      delay))

(defonce country-a2->country-a3
  (-> "http://country.io/iso3.json"
      slurp
      json/read-str
      delay))

(defonce country-a2->phone-code
  (-> "http://country.io/phone.json"
      slurp
      json/read-str
      delay))

(defonce country-a2->currency-code-a3
  (-> "http://country.io/currency.json"
      slurp
      json/read-str
      delay))

(defn country-data []
  (into {}
        (map #(vector % {:name (get @country-a2->country-name %)
                   :phone-code (get @country-a2->phone-code %)
                   :a3-code (get @country-a2->country-a3 %)
                   :capital (get @country-a2->capital %)
                   :continent-a2 (get @country-a2->continent-a2 %)
                   }))
        (keys @country-a2->country-name)))


(defonce currencies
  (-> "https://gist.githubusercontent.com/fightbulc/94f7d2cb602bc33eea1f/raw/10424dffd4c7db69b5de43b0e7124d6e25f7188f/iso-4217-minor-units.json" ;; TODO:
      slurp
      json/read-str
      delay))

(defonce currency-code->minor-units
  (-> (zipmap (map #(get % "Alphabetic Code") @currencies)
              (map #(try (Long/parseLong (get % "Minor unit"))
                         (catch NumberFormatException _ 0))
                   @currencies))
      (dissoc "")))

(defonce currency-code-a3->currency-code-num
  (-> (zipmap (map #(get % "Alphabetic Code") @currencies)
          (map #(get % "Numeric Code") @currencies))
      (dissoc ""))
  )
