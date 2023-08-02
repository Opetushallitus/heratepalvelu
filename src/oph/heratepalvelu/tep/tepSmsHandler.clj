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
           (java.time LocalDate)
           (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.tep.tepSmsHandler"
  :methods [[^:static handleTepSmsSending
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn update-status-to-db
  "Päivittää tiedot tietokantaan, kun SMS-viesti on lähetetty
  viestintäpalveluun. Parametrin status pitäisi olla string."
  [status puhelinnumero nippu new-loppupvm lahetyspvm]
  (log/info "Päivitetään tietokantaan status" status "lahetyspvm" lahetyspvm
            "uusi loppupvm" new-loppupvm)
  (try
    (let [updates {:sms_kasittelytila [:s status]
                   :sms_lahetyspvm    [:s lahetyspvm]
                   :sms_muistutukset  [:n 0]
                   :lahetettynumeroon [:s puhelinnumero]}
          updates (if new-loppupvm
                    (assoc updates :voimassaloppupvm [:s new-loppupvm])
                    updates)]
      (tc/update-nippu nippu updates))
    (catch Exception e
      (log/error (str "Error in update-status-to-db. Status:" status))
      (throw e))))

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
    (if (and (some? number) (c/valid-number? number))
      number
      (do
        (log/warn "Epäselvää, mihin numeroon nippu lähetetään; numero on"
                  number "jaksoissa" jaksot)
        (tc/update-nippu
          nippu
          (if (some? number)
            {:sms_kasittelytila [:s (:phone-invalid c/kasittelytilat)]
             :lahetettynumeroon [:s number]}
            (let [numerot (set (keep :ohjaaja_puhelinnumero jaksot))]
              {:sms_kasittelytila [:s (if (empty? numerot)
                                        (:no-phone c/kasittelytilat)
                                        (:phone-mismatch c/kasittelytilat))]})))
        (when (or (= (:email-mismatch c/kasittelytilat) (:kasittelytila nippu))
                  (= (:no-email c/kasittelytilat) (:kasittelytila nippu)))
          (log/info "Päivitetään tila Arvoon" (:kyselylinkki nippu))
          (arvo/patch-nippulinkki (:kyselylinkki nippu)
                                  {:tila (:ei-yhteystietoja c/kasittelytilat)}))
        nil))))

(defn query-lahetettavat
  "Hakee enintään limit nippua tietokannasta, joilta SMS-viesti ei ole vielä
  lähetetty ja niputuspäivämäärä on jo mennyt."
  []
  (ddb/query-items
    {:sms_kasittelytila [:eq [:s (:ei-lahetetty c/kasittelytilat)]]
     :niputuspvm    [:le [:s (str (c/local-date-now))]]}
    {:index "smsIndex"}
    (:nippu-table env)))

(defn -handleTepSmsSending
  "Hakee nippuja tietokannasta, joilta ei ole lähetetty SMS-viestejä, ja
  käsittelee viestien lähetystä."
  [_ event ^com.amazonaws.services.lambda.runtime.Context context]
  (log-caller-details-scheduled "tepSmsHandler" event context)
  (let [lahetettavat (query-lahetettavat)
        timeout? (c/no-time-left? context 60000)]
    (log/info "Aiotaan käsitellä" (count lahetettavat) "lähetettävää viestiä.")
    (when (seq lahetettavat)
      (c/doseq-with-timeout
        timeout?
        [nippu lahetettavat]
        (try
          (log/info "Lähetetään SMS nipulle" nippu)
          (if (= (:ei-niputettu c/kasittelytilat) (:kasittelytila nippu))
            (log/error "Nipulla on käsittelytila ei-niputettu")
            (if-not (c/has-time-to-answer? (:voimassaloppupvm nippu))
              (do
                (log/info "Vastausaika päättynyt" (:voimassaloppupvm nippu))
                (tc/update-nippu
                  nippu
                  {:sms_lahetyspvm [:s (str (c/local-date-now))]
                   :sms_kasittelytila
                   [:s (:vastausaika-loppunut c/kasittelytilat)]}))
              (let [jaksot (tc/get-jaksot-for-nippu nippu)
                    oppilaitokset (c/get-oppilaitokset jaksot)
                    puhelinnumero (ohjaaja-puhnro nippu jaksot)
                    sms-kasittelytila (:sms_kasittelytila nippu)]
                (if (or (nil? puhelinnumero)
                        (and (some? sms-kasittelytila)
                             (not= sms-kasittelytila
                                   (:ei-lahetetty c/kasittelytilat))))
                  (log/warn "SMS:a ei voi lähettää, numero" puhelinnumero
                            "käsittelytila" sms-kasittelytila)
                  (let [body (elisa/tep-msg-body (:kyselylinkki nippu)
                                                 oppilaitokset)
                        resp (elisa/send-sms puhelinnumero body)
                        status (get-in resp [:body
                                             :messages
                                             (keyword puhelinnumero)
                                             :status])
                        converted (get-in resp [:body
                                                :messages
                                                (keyword puhelinnumero)
                                                :converted])
                        new-loppupvm (tc/get-new-loppupvm nippu)
                        lahetyspvm (str (c/local-date-now))]
                    (log/info "SMS lähetetty, vastaus" resp)
                    (update-status-to-db status
                                         (or converted puhelinnumero)
                                         nippu
                                         new-loppupvm
                                         lahetyspvm)
                    (log/info "Päivitetään tiedot Arvoon" (:kyselylinkki nippu))
                    (arvo/patch-nippulinkki (:kyselylinkki nippu)
                                            (update-arvo-obj-sms status
                                                                 new-loppupvm))
                    (when-not (= (:niputuspvm nippu) lahetyspvm)
                      (log/warn "Nipun" (:ohjaaja_ytunnus_kj_tutkinto nippu)
                                "niputuspvm" (:niputuspvm nippu)
                                "ja sms-lahetyspvm" lahetyspvm
                                "eroavat toisistaan.")))))))
          (catch Exception e
            (log/error e "nipussa" nippu)))))))
