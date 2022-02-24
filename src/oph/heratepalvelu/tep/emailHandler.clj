(ns oph.heratepalvelu.tep.emailHandler
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.external.organisaatio :as org]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.tep.tepCommon :as tc])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))

;; Käsittelee TEP-jaksoja, joiden sähköpostit on määrä lähettää.

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

(defn lahetysosoite-update-item
  "Päivittää nipun käsittelytilan tietokantaan, kun osoitteista ei löydy selkeää
  vaihtoehtoa. Parametri osoitteet on lista sähköpostiosoitteista."
  [nippu osoitteet]
  (ddb/update-item
    {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]
     :niputuspvm                  [:s (:niputuspvm nippu)]}
    {:update-expr      "SET #kasittelytila = :kasittelytila"
     :expr-attr-names {"#kasittelytila" "kasittelytila"}
     :expr-attr-vals  {":kasittelytila" [:s (if (empty? osoitteet)
                                              (:no-email c/kasittelytilat)
                                              (:email-mismatch c/kasittelytilat))]}}
    (:nippu-table env)))

(defn get-single-ohjaaja-email
  "Jos jaksoissa on vain yksi ohjaajan sähköpostiosoite, palauttaa sen. Jos ei
  ole sähköpostiosoitetta tai on useita, palauttaa nil."
  [jaksot]
  (when (not-empty jaksot)
    (:ohjaaja_email (reduce #(if (some? (:ohjaaja_email %1))
                               (if (some? (:ohjaaja_email %2))
                                 (if (= (:ohjaaja_email %1) (:ohjaaja_email %2))
                                   %1
                                   (reduced nil))
                                  %1)
                                %2)
                              jaksot))))

(defn lahetysosoite
  "Yrittää hakea yksittäisen sähköpostiosoitteen nippuun liittyvistä jaksoista.
  Jos löytyy, palauttaa sen. Jos sitä ei löydy, päivittää tiedot tietokantaan
  ja palauttaa nil. Jos yksittäistä sahköpostia ei löydy eikä kunnon
  puhelinnumeroa on olemassa, ilmoittaa Arvoon, että ei ole yhteistietoja."
  [nippu jaksot]
  (let [ohjaaja-email (get-single-ohjaaja-email jaksot)]
    (if (some? ohjaaja-email)
      ohjaaja-email
      (let [osoitteet (reduce
                        #(if (some? (:ohjaaja_email %2))
                          (conj %1 (:ohjaaja_email %2))
                          %1)
                        #{} jaksot)]
        (log/warn "Ei yksiselitteistä ohjaajan sähköpostia "
                    (:ohjaaja_ytunnus_kj_tutkinto nippu) ","
                    (:niputuspvm nippu) "," osoitteet)
        (lahetysosoite-update-item nippu osoitteet)
        (when (bad-phone? nippu)
          (arvo/patch-nippulinkki (:kyselylinkki nippu) {:tila (:ei-yhteystietoja c/kasittelytilat)}))
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
    (ddb/update-item
      {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]
       :niputuspvm                  [:s (:niputuspvm nippu)]}
      {:update-expr     (str "SET #kasittelytila = :kasittelytila, "
                             "#vpid = :vpid, "
                             "#lahetyspvm = :lahetyspvm, "
                             "#muistutukset = :muistutukset, "
                             "#lahetysosoite = :lahetysosoite")
       :expr-attr-names {"#kasittelytila" "kasittelytila"
                         "#vpid" "viestintapalvelu-id"
                         "#lahetyspvm" "lahetyspvm"
                         "#muistutukset" "muistutukset"
                         "#lahetysosoite" "lahetysosoite"}
       :expr-attr-vals  {":kasittelytila" [:s (:viestintapalvelussa c/kasittelytilat)]
                         ":vpid" [:n id]
                         ":lahetyspvm" [:s lahetyspvm]
                         ":muistutukset" [:n 0]
                         ":lahetysosoite" [:s osoite]}}
      (:nippu-table env))
    (catch AwsServiceException e
      (log/error "Viestitiedot nipusta" nippu "ei päivitetty kantaan!")
      (log/error e))))

(defn send-survey-email
  "Lähettää sähköpostiviestin viestintäpalveluun. Parametri oppilaitokset on
  lista objekteista, joissa on ko. oppilaitoksen nimi kolmeksi eri kieleksi
  (avaimet :en, :fi, ja :sv); parametri osoite on sähköpostiosoite stringinä."
  [nippu oppilaitokset osoite]
  (try
    (vp/send-email {:subject "Työpaikkaohjaajakysely - Enkät till arbetsplatshandledaren - Survey to workplace instructors"
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
    (ddb/update-item
      {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]
       :niputuspvm                  [:s (:niputuspvm nippu)]}
      {:update-expr     (str "SET #kasittelytila = :kasittelytila, "
                             "#lahetyspvm = :lahetyspvm")
       :expr-attr-names {"#kasittelytila" "kasittelytila"
                         "#lahetyspvm" "lahetyspvm"}
       :expr-attr-vals {":kasittelytila" [:s (:vastausaika-loppunut c/kasittelytilat)]
                        ":lahetyspvm" [:s (str (c/local-date-now))]}}
      (:nippu-table env))
    (catch Exception e
      (log/error "Virhe lähetystilan päivityksessä nipulle, jonka vastausaika umpeutunut" nippu)
      (log/error e))))

(defn do-jakso-query
  "Hakee jaksot, jotka kuuluvat ko. nippuun."
  [nippu]
  (ddb/query-items {:ohjaaja_ytunnus_kj_tutkinto [:eq [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]]
                    :niputuspvm                  [:eq [:s (:niputuspvm nippu)]]}
                   {:index "niputusIndex"}
                   (:jaksotunnus-table env)))

(defn get-oppilaitokset
  "Hakee oppilaitosten nimet organisaatiopalvelusta jaksojen oppilaitos-kenttien
  perusteella."
  [jaksot]
  (try
    (seq (into #{} (map #(:nimi (org/get-organisaatio (:oppilaitos %1)))
                        jaksot)))
    (catch Exception e
      (log/error "Virhe kutsussa organisaatiopalveluun")
      (log/error e))))

(defn -handleSendTEPEmails
  "Hakee nippuja tietokannasta, joiden sähköpostit on aika lähettää, ja
  käsittelee näiden viestien lähettämisen viestinäpalveluun."
  [this event context]
  (log-caller-details-scheduled "handleSendTEPEmails" event context)
  (loop [lahetettavat (do-nippu-query)]
    (log/info "Käsitellään" (count lahetettavat) "lähetettävää viestiä.")
    (when (seq lahetettavat)
      (doseq [nippu lahetettavat]
        (let [jaksot (do-jakso-query nippu)
              oppilaitokset (get-oppilaitokset jaksot)
              osoite (lahetysosoite nippu jaksot)]
          (if (c/has-time-to-answer? (:voimassaloppupvm nippu))
            (when (some? osoite)
              (let [id (:id (send-survey-email nippu oppilaitokset osoite))
                    lahetyspvm (str (c/local-date-now))]
                (email-sent-update-item nippu id lahetyspvm osoite)))
            (no-time-to-answer-update-item nippu))))
      (when (< 60000 (.getRemainingTimeInMillis context))
        (recur (do-nippu-query))))))
