(ns oph.heratepalvelu.amis.AMISherateEmailHandler
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.log.caller-log :refer :all])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.amis.AMISherateEmailHandler"
  :methods [[^:static handleSendAMISEmails
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn save-email-to-db [email id lahetyspvm]
  (try
    (ddb/update-item
      {:toimija_oppija [:s (:toimija_oppija email)]
       :tyyppi_kausi   [:s (:tyyppi_kausi email)]}
      {:update-expr    (str "SET #lahetystila = :lahetystila, "
                            "#vpid = :vpid, "
                            "#lahetyspvm = :lahetyspvm, "
                            "#muistutukset = :muistutukset")
       :expr-attr-names {"#lahetystila" "lahetystila"
                         "#vpid" "viestintapalvelu-id"
                         "#lahetyspvm" "lahetyspvm"
                         "#muistutukset" "muistutukset"}
       :expr-attr-vals  {":lahetystila" [:s (:viestintapalvelussa c/kasittelytilat)]
                         ":vpid" [:n id]
                         ":lahetyspvm" [:s lahetyspvm]
                         ":muistutukset" [:n 0]}})
    (catch AwsServiceException e
      (log/error "Viesti" email "ei päivitetty kantaan")
      (log/error e))))

(defn update-data-in-ehoks [email lahetyspvm]
  (try
    (c/send-lahetys-data-to-ehoks
      (:toimija_oppija email)
      (:tyyppi_kausi email)
      {:kyselylinkki (:kyselylinkki email)
       :lahetyspvm lahetyspvm
       :sahkoposti (:sahkoposti email)
       :lahetystila (:viestintapalvelussa c/kasittelytilat)})
    (catch Exception e
      (log/error "Virhe tietojen päivityksessä ehoksiin:" email)
      (log/error e))))

(defn send-feedback-email [email]
  (try
    (vp/send-email {:subject "Palautetta oppilaitokselle - Respons till läroanstalten - Feedback to educational institution"
                    :body (vp/amispalaute-html email)
                    :address (:sahkoposti email)
                    :sender "Opetushallitus – Utbildningsstyrelsen – EDUFI"})
    (catch Exception e
      (log/error "Virhe palautesähköpostin lähetyksessä:" email)
      (log/error e))))

(defn save-no-time-to-answer [email]
  (try
    (ddb/update-item
      {:toimija_oppija [:s (:toimija_oppija email)]
       :tyyppi_kausi   [:s (:tyyppi_kausi email)]}
      {:update-expr     (str "SET #lahetystila = :lahetystila, "
                             "#lahetyspvm = :lahetyspvm")
       :expr-attr-names {"#lahetystila" "lahetystila"
                         "#lahetyspvm" "lahetyspvm"}
       :expr-attr-vals {":lahetystila" [:s (:vastausaika-loppunut c/kasittelytilat)]
                        ":lahetyspvm" [:s (str (c/local-date-now))]}})
    (catch Exception e
      (log/error "Virhe lähetystilan päivityksessä herätteelle, jonka vastausaika umpeutunut" email)
      (log/error e))))

(defn do-query []
  (ddb/query-items {:lahetystila [:eq [:s (:ei-lahetetty c/kasittelytilat)]]
                    :alkupvm     [:le [:s (str (c/local-date-now))]]}
                   {:index "lahetysIndex"
                    :limit 10}))

(defn -handleSendAMISEmails [this event context]
  (log-caller-details-scheduled "handleSendAMISEmails" event context)
  (loop [lahetettavat (do-query)]
    (log/info "Käsitellään " (count lahetettavat) " lähetettävää viestiä.")
    (when (seq lahetettavat)
      (doseq [email lahetettavat]
        (let [status (arvo/get-kyselylinkki-status (:kyselylinkki email))]
          (if (c/has-time-to-answer? (:voimassa_loppupvm status))
            (let [id (:id (send-feedback-email email))
                  lahetyspvm (str (c/local-date-now))]
              (save-email-to-db email id lahetyspvm)
              (update-data-in-ehoks email lahetyspvm))
            (save-no-time-to-answer email))))
      (when (< 60000 (.getRemainingTimeInMillis context))
        (recur (do-query))))))
