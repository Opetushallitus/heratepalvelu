(ns oph.heratepalvelu.tep.emailHandler
  "Käsittelee TEP-jaksoja, joiden sähköpostit on määrä lähettää."
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
  :name "oph.heratepalvelu.tep.emailHandler"
  :methods [[^:static handleSendTEPEmails
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn bad-phone?
  "Palauttaa true, jos nipun SMS-käsittelytilan mukaan puhelinnumero puuttuu tai
  on virheellinen."
  [nippu]
  (or (= (:phone-mismatch c/kasittelytilat) (:sms_kasittelytila nippu))
      (= (:no-phone c/kasittelytilat) (:sms_kasittelytila nippu))
      (= (:phone-invalid c/kasittelytilat) (:sms_kasittelytila nippu))))

(defn lahetysosoite
  "Yrittää hakea yksittäisen sähköpostiosoitteen nippuun liittyvistä jaksoista.
  Jos löytyy, palauttaa sen. Jos sitä ei löydy, päivittää tiedot tietokantaan
  ja palauttaa nil. Jos yksittäistä sahköpostia ei löydy eikä kunnon
  puhelinnumeroa on olemassa, ilmoittaa Arvoon, että ei ole yhteistietoja."
  [nippu jaksot]
  (let [ohjaaja-email (tc/reduce-common-value jaksot :ohjaaja_email)]
    (if (some? ohjaaja-email)
      ohjaaja-email
      (let [osoitteet (reduce #(if (some? (:ohjaaja_email %2))
                                 (conj %1 (:ohjaaja_email %2))
                                 %1)
                              #{}
                              jaksot)]
        (log/warn "Ei yksiselitteistä ohjaajan sähköpostia "
                  (:ohjaaja_ytunnus_kj_tutkinto nippu) ","
                  (:niputuspvm nippu) "," osoitteet)
        (let [kasittelytila (if (empty? osoitteet)
                              (:no-email c/kasittelytilat)
                              (:email-mismatch c/kasittelytilat))]
          (tc/update-nippu nippu {:kasittelytila [:s kasittelytila]}))
        (when (bad-phone? nippu)
          (arvo/patch-nippulinkki (:kyselylinkki nippu)
                                  {:tila (:ei-yhteystietoja c/kasittelytilat)}))
        nil))))

(defn do-nippu-query
  "Hakee nippuja tietokannasta, joiden sähköpostit on aika lähettää."
  []
  (ddb/query-items {:kasittelytila [:eq [:s (:ei-lahetetty c/kasittelytilat)]]
                    :niputuspvm    [:le [:s (str (c/local-date-now))]]}
                   {:index "niputusIndex"
                    :limit 20}
                   (:nippu-table env)))

(defn email-sent-update-item
  "Päivittää tiedot tietokantaan, kun sähköposti on lähetetty
  viestintäpalveluun. Parametri id on lähetyksen ID viestintäpalvelussa."
  [nippu id lahetyspvm osoite]
  (try
    (tc/update-nippu
      nippu
      {:kasittelytila       [:s (:viestintapalvelussa c/kasittelytilat)]
       :viestintapalvelu-id [:n id]
       :lahetyspvm          [:s lahetyspvm]
       :muistutukset        [:n 0]
       :lahetysosoite       [:s osoite]})
    (catch AwsServiceException e
      (log/error "Viestitiedot nipusta" nippu "ei päivitetty kantaan!")
      (log/error e))))

(defn send-survey-email
  "Lähettää sähköpostiviestin viestintäpalveluun. Parametri oppilaitokset on
  lista objekteista, joissa on ko. oppilaitoksen nimi kolmeksi eri kieleksi
  (avaimet :en, :fi, ja :sv); parametri osoite on sähköpostiosoite stringinä."
  [nippu oppilaitokset osoite]
  (try
    (vp/send-email {:subject (str "Työpaikkaohjaajakysely - "
                                  "Enkät till arbetsplatshandledaren - "
                                  "Survey to workplace instructors")
                    :body (vp/tyopaikkaohjaaja-html nippu oppilaitokset)
                    :address osoite
                    :sender "OPH – UBS – EDUFI"})
    (catch Exception e
      (log/error "Virhe viestin lähetyksessä!" nippu)
      (log/error e))))

(defn no-time-to-answer-update-item
  "Päivittää tietokantaan tiedot, että vastausaika on loppunut."
  [nippu]
  (try
    (tc/update-nippu
      nippu
      {:kasittelytila [:s (:vastausaika-loppunut c/kasittelytilat)]
       :lahetyspvm    [:s (str (c/local-date-now))]})
    (catch Exception e
      (log/error "Virhe lähetystilan päivityksessä nipulle,"
                 "jonka vastausaika umpeutunut"
                 nippu)
      (log/error e))))

(defn -handleSendTEPEmails
  "Hakee nippuja tietokannasta, joiden sähköpostit on aika lähettää, ja
  käsittelee näiden viestien lähettämisen viestinäpalveluun."
  [_ event ^com.amazonaws.services.lambda.runtime.Context context]
  (log-caller-details-scheduled "handleSendTEPEmails" event context)
  (loop [lahetettavat (do-nippu-query)]
    (log/info "Käsitellään" (count lahetettavat) "lähetettävää viestiä.")
    (when (seq lahetettavat)
      (doseq [nippu lahetettavat]
        (let [jaksot (tc/get-jaksot-for-nippu nippu)
              oppilaitokset (tc/get-oppilaitokset jaksot)
              osoite (lahetysosoite nippu jaksot)]
          (if (c/has-time-to-answer? (:voimassaloppupvm nippu))
            (when (some? osoite)
              (let [id (:id (send-survey-email nippu oppilaitokset osoite))
                    lahetyspvm (str (c/local-date-now))]
                (email-sent-update-item nippu id lahetyspvm osoite)
                (when-not (= (:niputuspvm nippu) lahetyspvm)
                  (log/warn
                    (str "Nipun "
                         (:ohjaaja_ytunnus_kj_tutkinto nippu)
                         " niputuspvm " (:niputuspvm nippu)
                         " ja lahetyspvm " lahetyspvm
                         " eroavat toisistaan.")))))
            (no-time-to-answer-update-item nippu))))
      (when (< 60000 (.getRemainingTimeInMillis context))
        (recur (do-nippu-query))))))
