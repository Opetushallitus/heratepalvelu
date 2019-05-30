(ns oph.heratepalvelu.herateEmailHandler
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.viestintapalvelu :refer [send-email]]
            [clojure.tools.logging :as log]
            [clj-time.core :refer [today]]
            [cheshire.core :refer [parse-string]])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.herateEmailHandler"
  :methods [[^:static handleSendEmails
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn -handleSendEmails [this event context]
    (let [lahetettavat (ddb/query-items {:lahetystila [:eq [:s "ei_lahetetty"]]
                                         :alkupvm     [:le [:s (.toString (today))]]}
                                        {:index "lahetysIndex"})]
      (log/info "Käsitellään " (count lahetettavat) " lähetettävää viestiä.")
      (doseq [email lahetettavat]
        (try
          (send-email email)
          (ddb/update-item {:toimija_oppija [:s (:toimija_oppija email)]
                             :tyyppi_kausi  [:s (:tyyppi_kausi email)]}
                            {:update-expr     "SET #lahetystila = :lahetystila"
                             :expr-attr-names {"#lahetystila" "lahetystila"}
                             :expr-attr-vals  {":lahetystila" [:s "lahetetty_viestintapalveluun"]}})
          (catch AwsServiceException e
            (log/error "Viesti lähetty viestintäpalveluun, muttei päivitetty kantaan!" e))
          (catch Exception e
            (log/error "Virhe viestin lähetyksessä!" email)
            (log/error e)))
        (log/info "Viesti käsitelty"))))