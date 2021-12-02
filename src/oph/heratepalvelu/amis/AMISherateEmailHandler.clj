(ns oph.heratepalvelu.amis.AMISherateEmailHandler
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.viestintapalvelu :refer [send-email amispalaute-html]]
            [oph.heratepalvelu.external.arvo :refer [get-kyselylinkki-status]]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.common :refer [has-time-to-answer? kasittelytilat send-lahetys-data-to-ehoks]]
            [clojure.tools.logging :as log]
            [clj-time.core :as t]
            [cheshire.core :refer [parse-string]])
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
       :expr-attr-vals  {":lahetystila" [:s (:viestintapalvelussa kasittelytilat)]
                         ":vpid" [:n id]
                         ":lahetyspvm" [:s lahetyspvm]
                         ":muistutukset" [:n 0]}})
    (catch AwsServiceException e
      (log/error "Viesti" email "ei päivitetty kantaan")
      (log/error e))))

(defn update-data-in-ehoks [email lahetyspvm]
  (try
    (send-lahetys-data-to-ehoks
      (:toimija_oppija email)
      (:tyyppi_kausi email)
      {:kyselylinkki (:kyselylinkki email)
       :lahetyspvm lahetyspvm
       :sahkoposti (:sahkoposti email)
       :lahetystila (:viestintapalvelussa kasittelytilat)})
    (catch Exception e
      (log/error "Virhe tietojen päivityksessä ehoksiin:" email)
      (log/error e))))

(defn send-feedback-email [email]
  (try
    (send-email {:subject "Palautetta oppilaitokselle - Respons till läroanstalten - Feedback to educational institution"
                 :body (amispalaute-html email)
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
       :expr-attr-vals {":lahetystila" [:s (:vastausaika-loppunut kasittelytilat)]
                        ":lahetyspvm" [:s (str (t/today))]}})
    (catch Exception e
      (log/error "Virhe lähetystilan päivityksessä herätteelle, jonka vastausaika umpeutunut" email)
      (log/error e))))

(defn do-query []
  (ddb/query-items {:lahetystila [:eq [:s (:ei-lahetetty kasittelytilat)]]
                    :alkupvm     [:le [:s (str (t/today))]]}
                   {:index "lahetysIndex"
                    :limit 10}))

(defn -handleSendAMISEmails [this event context]
  (log-caller-details-scheduled "handleSendAMISEmails" event context)
  (loop [lahetettavat (do-query)]
    (log/info "Käsitellään " (count lahetettavat) " lähetettävää viestiä.")
    (when (seq lahetettavat)
      (doseq [email lahetettavat]
        (let [status (get-kyselylinkki-status (:kyselylinkki email))]
          (if (has-time-to-answer? (:voimassa_loppupvm status))
            (let [id (:id (send-feedback-email email))
                  lahetyspvm (str (t/today))]
              (save-email-to-db email id lahetyspvm)
              (update-data-in-ehoks email lahetyspvm))
            (save-no-time-to-answer email))))
      (when (< 60000 (.getRemainingTimeInMillis context))
        (recur (do-query))))))
