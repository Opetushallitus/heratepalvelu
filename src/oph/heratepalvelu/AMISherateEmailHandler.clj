(ns oph.heratepalvelu.AMISherateEmailHandler
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.viestintapalvelu :refer [send-email amispalaute-html]]
            [oph.heratepalvelu.external.arvo :refer [get-kyselylinkki-status]]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.common :refer [has-time-to-answer?]]
            [clojure.tools.logging :as log]
            [clj-time.core :as t]
            [cheshire.core :refer [parse-string]])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.AMISherateEmailHandler"
  :methods [[^:static handleSendAMISEmails
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn -handleSendAMISEmails [this event context]
  (log-caller-details "handleSendAMISEmails" event context)
  (loop [lahetettavat (ddb/query-items {:lahetystila [:eq [:s "ei_lahetetty"]]
                                        :alkupvm     [:le [:s (.toString (t/today))]]}
                                       {:index "lahetysIndex"
                                        :limit 100})]
    (log/info "Käsitellään " (count lahetettavat) " lähetettävää viestiä.")
    (when (seq lahetettavat)
      (doseq [email lahetettavat]
        (let [status (get-kyselylinkki-status (:kyselylinkki email))]
          (if (has-time-to-answer? (:voimassa_loppupvm status))
            (try
              (let [id (:id (send-email {:subject "Palautetta oppilaitokselle - Respons till läroanstalten - Feedback to educational institution"
                                         :body (amispalaute-html email)
                                         :address (:sahkoposti email)}))]
                (ddb/update-item
                  {:toimija_oppija [:s (:toimija_oppija email)]
                   :tyyppi_kausi   [:s (:tyyppi_kausi email)]}
                  {:update-expr     (str "SET #lahetystila = :lahetystila, "
                                         "#vpid = :vpid, "
                                         "#lahetyspvm = :lahetyspvm, "
                                         "#muistutukset = :muistutukset")
                   :expr-attr-names {"#lahetystila" "lahetystila"
                                     "#vpid" "viestintapalvelu-id"
                                     "#lahetyspvm" "lahetyspvm"
                                     "#muistutukset" "muistutukset"}
                   :expr-attr-vals  {":lahetystila" [:s "viestintapalvelussa"]
                                     ":vpid" [:n id]
                                     ":lahetyspvm" [:s (str (t/today))]
                                     ":muistutukset" [:n 0]}}))
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
                {:update-expr     (str "SET #lahetystila = :lahetystila, "
                                       "#lahetyspvm = :lahetyspvm")
                 :expr-attr-names {"#lahetystila" "lahetystila"
                                   "#lahetyspvm" "lahetyspvm"}
                 :expr-attr-vals {":lahetystila" [:s "vastausaika_loppunut_ennen_lahetysta"]
                                  ":lahetyspvm" [:s (str (t/today))]}})
              (catch Exception e
                (log/error "Virhe lähetystilan päivityksessä herätteelle, jonka vastausaika umpeutunut" email)
                (log/error e))))))
      (when (< 30000 (.getRemainingTimeInMillis context))
        (recur (ddb/query-items {:lahetystila [:eq [:s "ei_lahetetty"]]
                                 :alkupvm     [:le [:s (.toString (t/today))]]}
                                {:index "lahetysIndex"
                                 :limit 100}))))))
