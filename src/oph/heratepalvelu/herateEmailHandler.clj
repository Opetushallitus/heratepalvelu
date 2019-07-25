(ns oph.heratepalvelu.herateEmailHandler
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.viestintapalvelu :refer [send-email]]
            [clojure.tools.logging :as log]
            [clj-time.core :refer [today now]]
            [cheshire.core :refer [parse-string]])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)
           (fi.vm.sade.utils.cas CasClientException)))

(gen-class
  :name "oph.heratepalvelu.herateEmailHandler"
  :methods [[^:static handleSendEmails
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn -handleSendEmails [this event context]
  (loop [lahetettavat (ddb/query-items {:lahetystila [:eq [:s "ei_lahetetty"]]
                                        :alkupvm     [:le [:s (.toString (today))]]}
                                       {:index "lahetysIndex"
                                        :limit 100})]
    (log/info "Käsitellään " (count lahetettavat) " lähetettävää viestiä.")
    (when (not-empty lahetettavat)
      (doseq [email lahetettavat]
        (try
          (let [time (System/currentTimeMillis)
                id (:id (send-email email))]
            (try
              (ddb/update-item
                {:toimija_oppija [:s (:toimija_oppija email)]
                 :tyyppi_kausi   [:s (:tyyppi_kausi email)]}
                {:update-expr     (str "SET #lahetystila = :lahetystila, "
                                       "SET #vp-id = :vp-id")
                 :expr-attr-names {"#lahetystila" "lahetystila"
                                   "#vp-id" "viestintapalvelu-id"}
                 :expr-attr-vals  {":lahetystila" [:s (str "viestintapalvelussa")]
                                   ":vp-id" [:n id]}})
              (catch AwsServiceException e
                (log/error "Viesti " id " lähetty viestintäpalveluun, muttei päivitetty kantaan!")
                (log/error e)))
            (log/info "Viesti lähetetty ja lähetystila tallennettu tietokantaan, id " id
                      " Aika: " (- (System/currentTimeMillis) time) "ms"))
          (catch Exception e
            (log/error "Virhe viestin lähetyksessä!" email)
            (log/error e))))
      (when (> 30000 (.getRemainingTimeInMillis context))
        (recur (ddb/query-items {:lahetystila [:eq [:s "ei_lahetetty"]]
                                 :alkupvm     [:le [:s (.toString (today))]]}
                                {:index "lahetysIndex"
                                 :limit 100}))))))