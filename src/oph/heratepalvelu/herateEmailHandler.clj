(ns oph.heratepalvelu.herateEmailHandler
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.viestintapalvelu :refer [send-email]]
            [clojure.tools.logging :as log]
            [clj-time.core :refer [today]]
            [cheshire.core :refer [parse-string]])
  (:import (com.amazonaws AmazonServiceException)))

(gen-class
  :name "oph.heratepalvelu.herateEmailHandler"
  :methods [[^:static handleSendEmails
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn -handleSendEmails [this event context]
    (let [lahetettavat (ddb/query-items {:lahetystila [:eq "ei_lahetetty"]
                                         :alkupvm     [:le (.toString (today))]}
                                        {:index "lahetysIndex"})]
      (log/info "Käsitellään " (count lahetettavat) " lähetettävää viestiä.")
      (doseq [email lahetettavat]
        (log/info "Aloitetaan viestin käsittely")
        (try
          (send-email email)
          (log/info "Päivitetään lähetystila tietokantaan")
          (ddb/update-item {:oppija_toimija (:oppija_toimija email)
                             :tyyppi_kausi   (:tyyppi_kausi email)}
                            {:update-expr     "SET #name = :value"
                             :expr-attr-names {"#name" "lahetystila"}
                             :expr-attr-vals  {":value" "lahetetty_viestintapalveluun"}
                             :return          :none})
          (log/info "Lähetystila päivitetty")
          (catch AmazonServiceException e
            (log/error "Viesti lähetty viestintäpalveluun, muttei päivitetty kantaan!" e))
          (catch Exception e
            (log/error "Virhe viestin lähetyksessä!" email)
            (log/error e)))
        (log/info "Viesti käsitelty"))))