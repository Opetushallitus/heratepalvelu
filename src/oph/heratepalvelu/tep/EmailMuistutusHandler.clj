(ns oph.heratepalvelu.tep.EmailMuistutusHandler
  "Käsittelee ja lähettää TEP-sähköpostimuistutuksia"
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.tep.tepCommon :as tc])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.tep.EmailMuistutusHandler"
  :methods [[^:static handleSendEmailMuistutus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn send-reminder-email
  "Lähettää muistutusviestin viestintäpalveluun. Parametrin oppilaitokset tulee
  olla lista objekteista, joissa on oppilaitoksen nimi kolmeksi eri kieleksi
  (avaimet :en, :fi ja :sv)."
  [nippu oppilaitokset]
  (try
    (vp/send-email {:subject (str "Muistutus-påminnelse-reminder: "
                                  "Työpaikkaohjaajakysely - "
                                  "Enkät till arbetsplatshandledaren - "
                                  "Survey to workplace instructors")
                    :body (vp/tyopaikkaohjaaja-muistutus-html nippu
                                                              oppilaitokset)
                    :address (:lahetysosoite nippu)
                    :sender "OPH – UBS – EDUFI"})
    (catch Exception e
      (log/error "Virhe muistutuksen lähetyksessä!" nippu)
      (log/error e))))

(defn update-item-email-sent
  "Päivittää tiedot tietokantaan, kun muistutusviesti on lähetetty. Parametri id
  on lähetyksen ID viestintäpalvelussa."
  [nippu id]
  (try
    (tc/update-nippu
      nippu
      {:muistutus-viestintapalvelu-id [:n id]
       :email_muistutuspvm  [:s (str (c/local-date-now))]
       :muistutukset        [:n 1]})
    (catch AwsServiceException e
      (log/error "Muistutus" nippu "ei päivitetty kantaan!")
      (log/error e))))

(defn update-item-cannot-answer
  "Päivittää tiedot tietokantaan, jos kyselyyn ei voi enää vastata koska siihen
  on jo vastattu tai vastausaika on umpeutunut. Parametri status on objekti,
  jossa pitää olla boolean-arvo avaimella :vastattu."
  [nippu status]
  (try
    (let [kasittelytila (if (:vastattu status)
                          (:vastattu c/kasittelytilat)
                          (:vastausaika-loppunut-m c/kasittelytilat))]
      (tc/update-nippu nippu {:kasittelytila [:s kasittelytila]
                              :muistutukset  [:n 1]}))
    (catch Exception e
      (log/error "Virhe lähetystilan päivityksessä nippulinkille,"
                 "johon on vastattu tai jonka vastausaika umpeutunut"
                 nippu)
      (log/error status)
      (log/error e))))

(defn sendEmailMuistutus
  "Käsittelee ryhmää muistutuksia. Hakee niiden statuksia Arvosta ja lähettää
  ne, jos niihin ei ole vastattu ja vastausaika ei ole loppunut."
  [timeout? muistutettavat]
  (log/info "Aiotaan käsitellä" (count muistutettavat) "muistutusta.")
  (c/doseq-with-timeout
    timeout?
    [nippu muistutettavat]
    (try
      (log/info "Käsitellään kyselylinkki" (:kyselylinkki nippu))
      (let [status (arvo/get-nippulinkki-status (:kyselylinkki nippu))]
        (log/info "Arvo-status:" status)
        (if (and (not (:vastattu status))
                 (c/has-time-to-answer? (:voimassa_loppupvm status)))
          (let [jaksot (tc/get-jaksot-for-nippu nippu)
                oppilaitokset (c/get-oppilaitokset jaksot)
                id (:id (send-reminder-email nippu oppilaitokset))]
            (log/info "Lähetetty muistutusviesti" id)
            (update-item-email-sent nippu id))
          (update-item-cannot-answer nippu status)))
      (catch Exception e
        (log/error e "nipussa" nippu)))))

(defn query-muistutukset
  "Hakee tietokannasta nippuja, joista on aika lähettää muistutus."
  []
  (ddb/query-items
    {:muistutukset [:eq [:n 0]]
     :lahetyspvm   [:between [[:s (str (.minusDays (c/local-date-now) 10))]
                              [:s (str (.minusDays (c/local-date-now) 5))]]]}
    {:index "emailMuistutusIndex"}
    (:nippu-table env)))

(defn -handleSendEmailMuistutus
  "Käsittelee muistettavia nippuja."
  [_ event ^com.amazonaws.services.lambda.runtime.Context context]
  (log-caller-details-scheduled "handleSendEmailMuistutus" event context)
  (sendEmailMuistutus (c/no-time-left? context 60000)
                      (query-muistutukset)))
