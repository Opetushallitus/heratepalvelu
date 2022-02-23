(ns oph.heratepalvelu.tep.tepSmsHandler
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.external.elisa :as elisa]
            [oph.heratepalvelu.external.organisaatio :as org]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.tep.tepCommon :as tc])
  (:import (clojure.lang ExceptionInfo)
           (com.google.i18n.phonenumbers PhoneNumberUtil NumberParseException)
           (java.time LocalDate)
           (software.amazon.awssdk.awscore.exception AwsServiceException)))

;; Käsittelee nippuihin liittyvät SMS-lähetykset

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
      (ddb/update-item
        {:ohjaaja_ytunnus_kj_tutkinto [:s ohjaaja_ytunnus_kj_tutkinto]
         :niputuspvm                  [:s niputuspvm]}
        {:update-expr     (str "SET #sms_kasittelytila = :sms_kasittelytila, "
                               "#sms_lahetyspvm = :sms_lahetyspvm, "
                               "#sms_muistutukset = :sms_muistutukset, "
                               "#lahetettynumeroon = :lahetettynumeroon, "
                               "#loppupvm = :loppupvm")
         :expr-attr-names {"#sms_kasittelytila" "sms_kasittelytila"
                           "#sms_lahetyspvm" "sms_lahetyspvm"
                           "#sms_muistutukset" "sms_muistutukset"
                           "#lahetettynumeroon" "lahetettynumeroon"
                           "#loppupvm" "voimassaloppupvm"}
         :expr-attr-vals  {":sms_kasittelytila" [:s status]
                           ":sms_lahetyspvm"    [:s (str (c/local-date-now))]
                           ":sms_muistutukset"  [:n 0]
                           ":lahetettynumeroon" [:s puhelinnumero]
                           ":loppupvm"          [:s (if new-loppupvm
                                                      new-loppupvm
                                                      (:voimassaloppupvm nippu))]}}
        (:nippu-table env))
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
    (if (some? number)
      (if (valid-number? number)
        number
        (do
          (ddb/update-item
            {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]
             :niputuspvm                  [:s (:niputuspvm nippu)]}
            {:update-expr     (str "SET #sms_kasittelytila = :sms_kasittelytila, "
                                   "#lahetettynumeroon = :lahetettynumeroon")
             :expr-attr-names {"#sms_kasittelytila" "sms_kasittelytila"
                               "#lahetettynumeroon" "lahetettynumeroon"}
             :expr-attr-vals {":sms_kasittelytila" [:s (:phone-invalid c/kasittelytilat)]
                              ":lahetettynumeroon" [:s number]}}
            (:nippu-table env))
          (when (or (= (:email-mismatch c/kasittelytilat) (:kasittelytila nippu))
                    (= (:no-email c/kasittelytilat) (:kasittelytila nippu)))
            (arvo/patch-nippulinkki (:kyselylinkki nippu) {:tila (:ei-yhteystietoja c/kasittelytilat)}))
          nil))
      (let [numerot (reduce #(if (some? (:ohjaaja_puhelinnumero %2))
                               (conj %1 (:ohjaaja_puhelinnumero %2))
                               %1) #{} jaksot)]
        (log/warn "Ei yksiselitteistä ohjaajan puhelinnumeroa, " (count numerot) " numeroa löydetty")
        (ddb/update-item
          {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]
           :niputuspvm                  [:s (:niputuspvm nippu)]}
          {:update-expr     (str "SET #sms_kasittelytila = :sms_kasittelytila")
           :expr-attr-names {"#sms_kasittelytila" "sms_kasittelytila"}
           :expr-attr-vals {":sms_kasittelytila" [:s (if (empty? numerot)
                                                       (:no-phone c/kasittelytilat)
                                                       (:phone-mismatch c/kasittelytilat))]}}
          (:nippu-table env))
        (when (or (= (:email-mismatch c/kasittelytilat) (:kasittelytila nippu))
                  (= (:no-email c/kasittelytilat) (:kasittelytila nippu)))
          (arvo/patch-nippulinkki (:kyselylinkki nippu) {:tila (:ei-yhteystietoja c/kasittelytilat)}))
        nil))))

(defn client-error?
  "Tarkistaa, onko virheen statuskoodi 4xx-haitarissa."
  [e]
  (and (> (:status (ex-data e)) 399)
       (< (:status (ex-data e)) 500)))

(defn update-vastausaika-loppunut-to-db
  "Päivittää tietokantaan sen, että vastausaika on loppunut."
  [nippu]
  (ddb/update-item
    {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]
     :niputuspvm                  [:s (:niputuspvm nippu)]}
    {:update-expr     (str "SET #sms_kasittelytila = :sms_kasittelytila, "
                           "#sms_lahetyspvm = :sms_lahetyspvm")
     :expr-attr-names {"#sms_kasittelytila" "sms_kasittelytila"
                       "#sms_lahetyspvm" "sms_lahetyspvm"}
     :expr-attr-vals {":sms_kasittelytila" [:s (:vastausaika-loppunut c/kasittelytilat)]
                      ":sms_lahetyspvm" [:s (str (c/local-date-now))]}}
    (:nippu-table env)))

(defn query-lahetettavat
  "Hakee enintään limit nippua tietokannasta, joilta SMS-viesti ei ole vielä
  lähetetty ja niputuspäivämäärä on jo mennyt."
  [limit]
  (ddb/query-items {:sms_kasittelytila [:eq [:s (:ei-lahetetty c/kasittelytilat)]]
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
                  oppilaitokset (seq (into #{}
                                           (map
                                             #(:nimi (org/get-organisaatio (:oppilaitos %1)))
                                             jaksot)))
                  puhelinnumero (ohjaaja-puhnro nippu jaksot)
                  sms-kasittelytila (:sms_kasittelytila nippu)]
              (when (and (some? puhelinnumero)
                         (or (nil? sms-kasittelytila)
                             (= sms-kasittelytila (:ei-lahetetty c/kasittelytilat))))
                (try
                  (let [body (elisa/msg-body (:kyselylinkki nippu) oppilaitokset)
                        resp (elisa/send-tep-sms puhelinnumero body)
                        status (get-in resp [:body :messages (keyword puhelinnumero) :status])
                        converted (get-in resp [:body :messages (keyword puhelinnumero) :converted])
                        new-loppupvm (tc/get-new-loppupvm nippu)]
                    (update-status-to-db status (or converted puhelinnumero) nippu new-loppupvm)
                    (arvo/patch-nippulinkki (:kyselylinkki nippu)
                                            (update-arvo-obj-sms status new-loppupvm)))
                  (catch AwsServiceException e
                    (log/error (str "SMS-viestin lähetysvaiheen kantapäivityksessä tapahtui virhe!"))
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
              (update-vastausaika-loppunut-to-db nippu)
              (catch Exception e
                (log/error "Virhe sms-lähetystilan päivityksessä nipulle, jonka vastausaika umpeutunut")
                (log/error e))))))
      (when (< 60000 (.getRemainingTimeInMillis context))
        (recur (query-lahetettavat 10))))))
