(ns oph.heratepalvelu.amis.AMISherateEmailHandler
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.log.caller-log :refer :all])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))

;; Lähettää herätteiden sähköpostiviestit viestintäpalveluun.

(gen-class
  :name "oph.heratepalvelu.amis.AMISherateEmailHandler"
  :methods [[^:static handleSendAMISEmails
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn save-email-to-db
  "Tallentaa sähköpostin tiedot tietokantaan, kun sähköposti on lähetetty
  viestintäpalveluun."
  [herate id lahetyspvm]
  (try
    (ddb/update-item
      {:toimija_oppija [:s (:toimija_oppija herate)]
       :tyyppi_kausi   [:s (:tyyppi_kausi herate)]}
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
      (log/error "Tiedot herätteestä" herate "ei päivitetty kantaan")
      (log/error e))))

(defn update-data-in-ehoks
  "Päivittää sähköpostin tiedot ehoksiin, kun sähköposti on lähetetty
  viestintäpalveluun."
  [herate lahetyspvm]
  (try
    (c/send-lahetys-data-to-ehoks
      (:toimija_oppija herate)
      (:tyyppi_kausi herate)
      {:kyselylinkki (:kyselylinkki herate)
       :lahetyspvm lahetyspvm
       :sahkoposti (:sahkoposti herate)
       :lahetystila (:viestintapalvelussa c/kasittelytilat)})
    (catch Exception e
      (log/error "Virhe tietojen päivityksessä ehoksiin:" herate)
      (log/error e))))

(defn send-feedback-email
  "Lähettää palautekyselyviestin viestintäpalveluun."
  [herate]
  (try
    (vp/send-email {:subject "Palautetta oppilaitokselle - Respons till läroanstalten - Feedback to educational institution"
                    :body (vp/amispalaute-html herate)
                    :address (:sahkoposti herate)
                    :sender "Opetushallitus – Utbildningsstyrelsen – EDUFI"})
    (catch Exception e
      (log/error "Virhe palautesähköpostin lähetyksessä:" herate)
      (log/error e))))

(defn save-no-time-to-answer
  "Päivittää tietueen, jos herätteen vastausaika on umpeutunut."
  [herate]
  (try
    (ddb/update-item
      {:toimija_oppija [:s (:toimija_oppija herate)]
       :tyyppi_kausi   [:s (:tyyppi_kausi herate)]}
      {:update-expr     (str "SET #lahetystila = :lahetystila, "
                             "#lahetyspvm = :lahetyspvm")
       :expr-attr-names {"#lahetystila" "lahetystila"
                         "#lahetyspvm" "lahetyspvm"}
       :expr-attr-vals {":lahetystila" [:s (:vastausaika-loppunut c/kasittelytilat)]
                        ":lahetyspvm" [:s (str (c/local-date-now))]}})
    (catch Exception e
      (log/error "Virhe lähetystilan päivityksessä herätteelle,"
                 "jonka vastausaika umpeutunut:"
                 herate)
      (log/error e))))

(defn do-query
  "Hakee tietueita tietokannasta, joiden lähetystilat ovat 'ei lähetetty'."
  []
  (ddb/query-items {:lahetystila [:eq [:s (:ei-lahetetty c/kasittelytilat)]]
                    :alkupvm     [:le [:s (str (c/local-date-now))]]}
                   {:index "lahetysIndex"
                    :limit 10}))

(defn -handleSendAMISEmails
  "Hakee lähetettäviä herätteitä tietokannasta ja lähettää viestit
  viestintäpalveluun."
  [this event context]
  (log-caller-details-scheduled "handleSendAMISEmails" event context)
  (loop [lahetettavat (do-query)]
    (log/info "Käsitellään" (count lahetettavat) "lähetettävää viestiä.")
    (when (seq lahetettavat)
      (doseq [herate lahetettavat]
        (let [status (arvo/get-kyselylinkki-status (:kyselylinkki herate))]
          (if (c/has-time-to-answer? (:voimassa_loppupvm status))
            (let [id (:id (send-feedback-email herate))
                  lahetyspvm (str (c/local-date-now))]
              (save-email-to-db herate id lahetyspvm)
              (update-data-in-ehoks herate lahetyspvm))
            (save-no-time-to-answer herate))))
      (when (< 60000 (.getRemainingTimeInMillis context))
        (recur (do-query))))))
