(ns oph.heratepalvelu.herateEmailHandler
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.viestintapalvelu :refer [send-email]]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [clojure.tools.logging :as log]
            [clj-time.core :as t]
            [cheshire.core :refer [parse-string]]
            [clojure.string :as str])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.herateEmailHandler"
  :methods [[^:static handleSendEmails
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn has-time-to-answer? [alkupvm]
  (let [[years months days] (map #(Integer. %)
                                 (str/split alkupvm #"-"))]
    (t/before? (t/today)
               (t/plus (t/local-date years months days)
                       (t/days 29)))))

(defn -handleSendEmails [this event context]
  (log-caller-details "handleSendEmails" event context)
  (loop [lahetettavat (ddb/query-items {:lahetystila [:eq [:s "ei_lahetetty"]]
                                        :alkupvm     [:le [:s (.toString (t/today))]]}
                                       {:index "lahetysIndex"
                                        :limit 100})]
    (log/info "Käsitellään " (count lahetettavat) " lähetettävää viestiä.")
    (when (seq lahetettavat)
      (doseq [email lahetettavat]
        (if (has-time-to-answer? (:alkupvm email))
          (try
            (let [id (:id (send-email email))]
                (ddb/update-item
                  {:toimija_oppija [:s (:toimija_oppija email)]
                   :tyyppi_kausi   [:s (:tyyppi_kausi email)]}
                  {:update-expr     (str "SET #lahetystila = :lahetystila, "
                                         "#vpid = :vpid")
                   :expr-attr-names {"#lahetystila" "lahetystila"
                                     "#vpid" "viestintapalvelu-id"}
                   :expr-attr-vals  {":lahetystila" [:s "viestintapalvelussa"]
                                     ":vpid" [:n id]}}))
            (catch AwsServiceException e
              (log/error "Viesti " email " lähetty viestintäpalveluun, muttei päivitetty kantaan!")
              (log/error e))
            (catch Exception e
              (log/error "Virhe viestin lähetyksessä!" email)
              (log/error e)))
          (try
            (ddb/update-item
              {:toimija_oppija [:s (:toimija_oppija email)]
               :tyyppi_kausi   [:s (:tyyppi_kausi email)]}
              {:update-expr     "SET #lahetystila = :lahetystila"
               :expr-attr-names {"#lahetystila" "lahetystila"}
               :expr-attr-vals {":lahetystila" [:s "vastausaika_loppunut_ennen_lahetysta"]}})
            (catch Exception e
              (log/error "Virhe lähetystilan päivityksessä herätteelle, jonka vastausaika umpeutunut" email)
              (log/error e))))
        (log/info "Viesti käsitelty ja lähetystila tallennettu tietokantaan"))
      (when (< 30000 (.getRemainingTimeInMillis context))
        (recur (ddb/query-items {:lahetystila [:eq [:s "ei_lahetetty"]]
                                 :alkupvm     [:le [:s (.toString (t/today))]]}
                                {:index "lahetysIndex"
                                 :limit 100}))))))
