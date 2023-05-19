(ns oph.heratepalvelu.amis.AMISherateEmailHandler
  "Lähettää herätteiden sähköpostiviestit viestintäpalveluun."
  (:require [clojure.tools.logging :as log]
            [oph.heratepalvelu.amis.AMISCommon :as ac]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.external.ehoks :as ehoks]
            [oph.heratepalvelu.external.koski :as k]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.log.caller-log :refer :all])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.amis.AMISherateEmailHandler"
  :methods [[^:static handleSendAMISEmails
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn get-and-assoc-kyselylinkki
  "Hakee herätteen kyselylinkin Arvosta, päivittää sen herätteeseen
  tietokantaan, ja lisää sen herätteen. Jos kyselylinkki on jo olemassa,
  palauttaa herätteen ilman muutoksia; jos kyselylinkkiä ei voitu luoda,
  palauttaa nil."
  [herate]
  (if (:kyselylinkki herate)
    herate
    (if-let [opiskeluoikeus
             (if-let [oo (k/get-opiskeluoikeus-catch-404
                           (:opiskeluoikeus-oid herate))]
               oo
               (k/get-opiskeluoikeus-catch-404 (:opiskeluoikeus-oid herate)))]
      (if (c/feedback-collecting-prevented? opiskeluoikeus (:heratepvm herate))
        (do
          (log/info (str "Palautteen kerääminen estetty (opiskeluoikeus "
                         (:oid opiskeluoikeus)
                         ", ehoks-id "
                         (:ehoks-id herate)
                         ")"))
          (ac/update-herate
            herate
            {:lahetystila [:s (:ei-laheteta c/kasittelytilat)]
             :sms-lahetystila [:s (:ei-laheteta c/kasittelytilat)]})
          nil)
        (let [req-body (arvo/build-arvo-request-body
                         herate
                         opiskeluoikeus
                         (:request-id herate)
                         (:koulutustoimija herate)
                         (c/get-suoritus opiskeluoikeus)
                         (:alkupvm herate)
                         (:voimassa-loppupvm herate))
              arvo-resp (try
                          (if (= (:kyselytyyppi herate) "aloittaneet")
                            (arvo/create-amis-kyselylinkki req-body)
                            (arvo/create-amis-kyselylinkki-catch-404 req-body))
                          (catch Exception e
                            (log/error "Virhe kyselylinkin hakemisessa Arvosta."
                                       "Request:"
                                       req-body
                                       "Error:"
                                       e)
                            (throw e)))]
          (if-let [kyselylinkki (:kysely_linkki arvo-resp)]
            (do (ac/update-herate herate {:kyselylinkki [:s kyselylinkki]})
                (try
                  (ehoks/add-kyselytunnus-to-hoks
                    (:ehoks-id herate)
                    {:kyselylinkki kyselylinkki
                     :tyyppi       (:kyselytyyppi herate)
                     :alkupvm      (:alkupvm herate)
                     :lahetystila  (:ei-lahetetty c/kasittelytilat)})
                  (catch Exception e
                    (log/error "Virhe linkin lähetyksessä eHOKSiin " e)
                    (throw e)))
                (assoc herate :kyselylinkki kyselylinkki))
            (do (log/error "Kyselylinkkiä ei palautettu Arvosta. Request ID:"
                           (:request-id herate))
                (throw (ex-info "Kyselylinkkiä ei palautettu Arvosta."
                                {:request-id (:request-id herate)}))))))
      (do
        (ac/update-herate
          herate
          {:lahetystila [:s (:ei-laheteta-oo-ei-loydy c/kasittelytilat)]
           :sms-lahetystila [:s (:ei-laheteta-oo-ei-loydy c/kasittelytilat)]})
        nil))))

(defn save-email-to-db
  "Tallentaa sähköpostin tiedot tietokantaan, kun sähköposti on lähetetty
  viestintäpalveluun."
  [herate id lahetyspvm]
  (try
    (ac/update-herate herate
                      {:lahetystila [:s (:viestintapalvelussa c/kasittelytilat)]
                       :viestintapalvelu-id [:n id]
                       :lahetyspvm [:s lahetyspvm]
                       :muistutukset [:n 0]})
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
    (vp/send-email {:subject (str "Palautetta oppilaitokselle - "
                                  "Respons till läroanstalten - "
                                  "Feedback to educational institution")
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
    (ac/update-herate
      herate
      {:lahetystila [:s (:vastausaika-loppunut c/kasittelytilat)]
       :lahetyspvm  [:s (str (c/local-date-now))]})
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
  [_ event ^com.amazonaws.services.lambda.runtime.Context context]
  (log-caller-details-scheduled "handleSendAMISEmails" event context)
  (loop [lahetettavat (do-query)]
    (log/info "Käsitellään" (count lahetettavat) "lähetettävää viestiä.")
    (when (seq lahetettavat)
      (doseq [lahetettava lahetettavat]
        (when-let [herate (get-and-assoc-kyselylinkki lahetettava)]
          (let [status (arvo/get-kyselylinkki-status (:kyselylinkki herate))]
            (if (c/has-time-to-answer? (:voimassa_loppupvm status))
              (let [id (:id (send-feedback-email herate))
                    lahetyspvm (str (c/local-date-now))]
                (save-email-to-db herate id lahetyspvm)
                (update-data-in-ehoks herate lahetyspvm))
              (save-no-time-to-answer herate)))))
      (when (< 60000 (.getRemainingTimeInMillis context))
        (recur (do-query))))))
