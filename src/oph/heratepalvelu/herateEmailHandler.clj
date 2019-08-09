(ns oph.heratepalvelu.herateEmailHandler
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.viestintapalvelu :refer [send-email]]
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
  (loop [lahetettavat (ddb/query-items {:lahetystila [:eq [:s "ei_lahetetty"]]
                                        :alkupvm     [:le [:s (.toString (t/today))]]}
                                       {:index "lahetysIndex"
                                        :limit 100})]
    (log/info "Käsitellään " (count lahetettavat) " lähetettävää viestiä.")
    (when (seq lahetettavat)
      (doseq [email lahetettavat]
        (let [time (System/currentTimeMillis)
              id (:id (send-email email))]
          (try
            (if (has-time-to-answer? (:alkupvm email))
              (ddb/update-item
                {:toimija_oppija [:s (:toimija_oppija email)]
                 :tyyppi_kausi   [:s (:tyyppi_kausi email)]}
                {:update-expr     (str "SET #lahetystila = :lahetystila, "
                                       "SET #vp-id = :vp-id")
                 :expr-attr-names {"#lahetystila" "lahetystila"
                                   "#vp-id" "viestintapalvelu-id"}
                 :expr-attr-vals  {":lahetystila" [:s "viestintapalvelussa"]
                                     ":vp-id" [:n id]}})
              (ddb/update-item
                {:toimija_oppija [:s (:toimija_oppija email)]
                 :tyyppi_kausi   [:s (:tyyppi_kausi email)]}
                {:update-expr     "SET #lahetystila = :lahetystila"
                 :expr-attr-names {"#lahetystila" "lahetystila"}
                 :expr-attr-vals {":lahetystila" [:s "vastausaika_loppunut_ennen_lahetysta"]}}))
            (log/info "Viesti lähetetty ja lähetystila tallennettu tietokantaan, id " id
                      " Aika: " (- (System/currentTimeMillis) time) "ms")
            (catch AwsServiceException e
              (log/error "Viesti " id " lähetty viestintäpalveluun, muttei päivitetty kantaan!")
              (log/error e))
            (catch Exception e
              (log/error "Virhe viestin lähetyksessä!" email)
              (log/error e)))))
      (when (> 30000 (.getRemainingTimeInMillis context))
        (recur (ddb/query-items {:lahetystila [:eq [:s "ei_lahetetty"]]
                                 :alkupvm     [:le [:s (.toString (t/today))]]}
                                {:index "lahetysIndex"
                                 :limit 100}))))))