(ns oph.heratepalvelu.tep.tepSmsHandler
  "Käsittelee nippuihin liittyvät SMS-lähetykset."
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.external.elisa :as elisa]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.tep.tepCommon :as tc])
  (:import (clojure.lang ExceptionInfo)
           (com.google.i18n.phonenumbers PhoneNumberUtil NumberParseException)
           (java.time LocalDate)
           (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.tep.tepSmsHandler"
  :methods [[^:static handleTepSmsSending
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn valid-number?
  "Sallii vain numeroita, jotka kirjasto luokittelee mobiilinumeroiksi tai
  mahdollisiksi mobiilinumeroiksi (FIXED_LINE_OR_MOBILE). Jos funktio ei hyväksy
  numeroa, jonka tiedät olevan validi, tarkista, miten kirjasto luokittelee sen:
  https://libphonenumber.appspot.com/."
  [number]
  (try
    (let [utilobj (PhoneNumberUtil/getInstance)
          numberobj (.parse utilobj number "FI")]
      (and (empty? (filter (fn [x] (Character/isLetter x)) number))
           (.isValidNumber utilobj numberobj)
           (let [numtype (str (.getNumberType utilobj numberobj))]
             (or (= numtype "FIXED_LINE_OR_MOBILE") (= numtype "MOBILE")))))
    (catch NumberParseException e
      (log/error "PhoneNumberUtils failed to parse phonenumber")
      (log/error e)
      false)))

(defn update-status-to-db
  "Päivittää tiedot tietokantaan, kun SMS-viesti on lähetetty
  viestintäpalveluun. Parametrin status pitäisi olla string."
  [status puhelinnumero nippu new-loppupvm]
  (let [ohjaaja_ytunnus_kj_tutkinto (:ohjaaja_ytunnus_kj_tutkinto nippu)
        niputuspvm                  (:niputuspvm nippu)]
    (try
      (let [updates {:sms_kasittelytila [:s status]
                     :sms_lahetyspvm    [:s (str (c/local-date-now))]
                     :sms_muistutukset  [:n 0]
                     :lahetettynumeroon [:s puhelinnumero]}
            updates (if new-loppupvm
                      (assoc updates :voimassaloppupvm [:s new-loppupvm])
                      updates)]
        (tc/update-nippu nippu updates))
      (catch Exception e
        (log/error (str "Error in update-status-to-db. Status:" status))
        (throw e)))))

(defn update-arvo-obj-sms
  "Luo Arvon päivitysobjektin tilan ja uuden loppupäivämäärän perusteella."
  [status new-loppupvm]
  (if (or (= status "CREATED") (= status "mock-lahetys"))
    (if new-loppupvm
      {:tila (:success c/kasittelytilat)
       :voimassa_loppupvm new-loppupvm}
      {:tila (:success c/kasittelytilat)})
    {:tila (:failure c/kasittelytilat)}))

(defn ohjaaja-puhnro
  "Yrittää löytää yksittäisen ohjaajan puhelinnumeron jaksoista. Jos yksittäinen
  numero on olemassa ja on validi, se palautetaan. Jos numeroa ei ole, useita
  numeroita löytyy, tai numero on virheellinen, nämä tiedot tallennetaan
  tietokantaan ja funktio palauttaa nil. Jos yksittäistä numeroa ei löydy ja
  nippuun on merkattu että hyväksyttävää sähköpostiosoitettakaan ei ole
  löytynyt, päivittää myös Arvoon sen, että nippulinkillä ei ole yhteystietoja."
  [nippu jaksot]
  (let [number (tc/reduce-common-value jaksot :ohjaaja_puhelinnumero)]
    (if (and (some? number) (valid-number? number))
      number
      (do
        (tc/update-nippu
          nippu
          (if (some? number)
            {:sms_kasittelytila [:s (:phone-invalid c/kasittelytilat)]
             :lahetettynumeroon [:s number]}
            (let [numerot (reduce #(if (some? (:ohjaaja_puhelinnumero %2))
                                     (conj %1 (:ohjaaja_puhelinnumero %2))
                                     %1)
                                  #{}
                                  jaksot)]
              (log/warn "Ei yksiselitteistä ohjaajan puhelinnumeroa,"
                        (count numerot)
                        "numeroa löydetty")
              {:sms_kasittelytila [:s (if (empty? numerot)
                                        (:no-phone c/kasittelytilat)
                                        (:phone-mismatch c/kasittelytilat))]})))
        (when (or (= (:email-mismatch c/kasittelytilat) (:kasittelytila nippu))
                  (= (:no-email c/kasittelytilat) (:kasittelytila nippu)))
          (arvo/patch-nippulinkki (:kyselylinkki nippu)
                                  {:tila (:ei-yhteystietoja c/kasittelytilat)}))
        nil))))

(defn client-error?
  "Tarkistaa, onko virheen statuskoodi 4xx-haitarissa."
  [e]
  (and (> (:status (ex-data e)) 399)
       (< (:status (ex-data e)) 500)))

(defn query-lahetettavat
  "Hakee enintään limit nippua tietokannasta, joilta SMS-viesti ei ole vielä
  lähetetty ja niputuspäivämäärä on jo mennyt."
  [limit]
  (ddb/query-items
    {:sms_kasittelytila [:eq [:s (:ei-lahetetty c/kasittelytilat)]]
     :niputuspvm    [:le [:s (str (c/local-date-now))]]}
    {:index "smsIndex"
     :limit limit}
    (:nippu-table env)))

(defn -handleTepSmsSending
  "Hakee nippuja tietokannasta, joilta ei ole lähetetty SMS-viestejä, ja
  käsittelee viestien lähetystä."
  [this event context]
  (log-caller-details-scheduled "tepSmsHandler" event context)
  (loop [lahetettavat (query-lahetettavat 20)]
    (log/info "Käsitellään" (count lahetettavat) "lähetettävää viestiä.")
    (when (seq lahetettavat)
      (doseq [nippu lahetettavat]
        (when-not (= (:ei-niputettu c/kasittelytilat) (:kasittelytila nippu))
          (if (c/has-time-to-answer? (:voimassaloppupvm nippu))
            (let [jaksot (tc/get-jaksot-for-nippu nippu)
                  oppilaitokset (tc/get-oppilaitokset jaksot)
                  puhelinnumero (ohjaaja-puhnro nippu jaksot)
                  sms-kasittelytila (:sms_kasittelytila nippu)]
              (when (and (some? puhelinnumero)
                         (or (nil? sms-kasittelytila)
                             (= sms-kasittelytila
                                (:ei-lahetetty c/kasittelytilat))))
                (try
                  (let [body (elisa/msg-body (:kyselylinkki nippu)
                                             oppilaitokset)
                        resp (elisa/send-tep-sms puhelinnumero body)
                        status (get-in resp [:body
                                             :messages
                                             (keyword puhelinnumero)
                                             :status])
                        converted (get-in resp [:body
                                                :messages
                                                (keyword puhelinnumero)
                                                :converted])
                        new-loppupvm (tc/get-new-loppupvm nippu)]
                    (update-status-to-db status
                                         (or converted puhelinnumero)
                                         nippu
                                         new-loppupvm)
                    (arvo/patch-nippulinkki (:kyselylinkki nippu)
                                            (update-arvo-obj-sms status
                                                                 new-loppupvm)))
                  (catch AwsServiceException e
                    (log/error "SMS-viestin lähetysvaiheen kantapäivityksessä"
                               "tapahtui virhe!")
                    (log/error e))
                  (catch ExceptionInfo e
                    (if (client-error? e)
                      (do
                        (log/error "Client error while sending sms")
                        (log/error e))
                      (do
                        (log/error "Server error while sending sms")
                        (log/error e))))
                  (catch Exception e
                    (log/error "Unhandled exception " e)))))
            (try
              (tc/update-nippu nippu
                               {:sms_lahetyspvm [:s (str (c/local-date-now))]
                                :sms_kasittelytila
                                [:s (:vastausaika-loppunut c/kasittelytilat)]})
              (catch Exception e
                (log/error "Virhe sms-lähetystilan päivityksessä nipulle,"
                           "jonka vastausaika umpeutunut")
                (log/error e))))))
      (when (< 60000 (.getRemainingTimeInMillis context))
        (recur (query-lahetettavat 10))))))
