(ns oph.heratepalvelu.amis.AMISMuistutusHandler
  "Käsittelee sähköpostimuistutuksia ja lähettää viestit viestintäpalveluun, jos
  kyselyyn ei ole vastattu ja vastausaika ei ole umpeutunut."
  (:require [clojure.tools.logging :as log]
            [oph.heratepalvelu.amis.AMISCommon :as ac]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.log.caller-log :refer :all])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)
           (clojure.lang ExceptionInfo)))

(gen-class
  :name "oph.heratepalvelu.amis.AMISMuistutusHandler"
  :methods [[^:static handleSendAMISMuistutus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn update-after-send
  "Päivittää sähköposti- ja herätetiedot tietokantaan, kun muistutus on
  lähetetty viestintäpalveluun."
  [herate n id]
  (try
    (ac/update-herate
      herate
      {:muistutukset [:n n]
       :viestintapalvelu-id [:n id]
       :lahetystila [:s (:viestintapalvelussa c/kasittelytilat)]
       (keyword (str n ".-muistutus-lahetetty")) [:s (str (c/local-date-now))]})
    (catch AwsServiceException e
      (log/error "Muistutus herätteelle"
                 herate
                 "lähetetty viestintäpalveluun, muttei päivitetty kantaan!")
      (log/error e))))

(defn update-when-not-sent
  "Päivittää herätteen tilan tietokantaan, kun muistutusta ei lähetetty."
  [herate n status]
  (let [tila (if (:vastattu status)
               (:vastattu c/kasittelytilat)
               (:vastausaika-loppunut-m c/kasittelytilat))]
    (ac/update-herate herate {:lahetystila [:s tila] :muistutukset [:n n]})))

(defn send-reminder-email
  "Lähettää muistutusviestin viestintäpalveluun."
  [herate]
  (vp/send-email
    {:subject (str "Muistutus-påminnelse-reminder: "
                   "Vastaa kyselyyn - svara på enkäten - answer the survey")
     :body (vp/amismuistutus-html herate)
     :address (:sahkoposti herate)
     :sender "Opetushallitus – Utbildningsstyrelsen – EDUFI"}))

(defn sendAMISMuistutus
  "Lähettää muistutusviestin ja tallentaa sen tilan tietokantaan, jos kyselyyn
  ei ole vastattu ja vastausaika ei ole umpeutunut."
  [timeout? muistutettavat n]
  (log/info "Aiotaan käsitellä" (count muistutettavat) "lähetettävää"
            (str n ".") "muistutusta.")
  (c/doseq-with-timeout
    timeout?
    [herate muistutettavat]
    (log/info "Käsitellään heräte" herate)
    (try
      (let [status (arvo/get-kyselylinkki-status (:kyselylinkki herate))
            still-open? (c/has-time-to-answer? (:voimassa_loppupvm status))]
        (log/info "Arvo-status" status)
        (if (and (not (:vastattu status)) still-open?)
          (let [id (:id (send-reminder-email herate))]
            (log/info "Ei vielä vastattu, lähetetään muistutus")
            (update-after-send herate n id))
          (update-when-not-sent herate n status)))
      (catch ExceptionInfo e
        (if (= 404 (:status (ex-data e)))
          (do
            (log/warn "Kyselylinkkiä ei löytynyt!  Merkitään loppuneeksi.")
            (update-when-not-sent herate n {}))
          (log/error e "tiedot:" (ex-data e))))
      (catch Exception e (log/error e "herätteellä" herate)))))

(defn query-muistutukset
  "Hakee tietokannasta herätteet, joilla on lähetettäviä muistutusviestejä."
  [n]
  (ddb/query-items {:muistutukset [:eq [:n (- n 1)]]
                    :lahetyspvm  [:between
                                  [[:s (str (.minusDays (c/local-date-now)
                                                        (- (* 5 (+ n 1)) 1)))]
                                   [:s (str (.minusDays (c/local-date-now)
                                                        (* 5 n)))]]]}
                   {:index "muistutusIndex"}))

(defn -handleSendAMISMuistutus
  "Käsittelee AMISin muistutusviestien lähetystä."
  [_ event ^com.amazonaws.services.lambda.runtime.Context context]
  (log-caller-details-scheduled "handleSendAMISMuistutus" event context)
  (doseq [kerta [1 2]]
    (sendAMISMuistutus (c/no-time-left? context 60000)
                       (query-muistutukset kerta)
                       kerta)))
