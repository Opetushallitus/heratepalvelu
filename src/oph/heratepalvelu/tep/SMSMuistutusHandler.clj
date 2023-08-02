(ns oph.heratepalvelu.tep.SMSMuistutusHandler
  "Käsittelee ja lähettää SMS-muistutukset"
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.external.elisa :as elisa]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.tep.tepCommon :as tc])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)
           (java.time LocalDate)))

(gen-class
  :name "oph.heratepalvelu.tep.SMSMuistutusHandler"
  :methods [[^:static handleSendSMSMuistutus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn sendSmsMuistutus
  "Hakee jaksot ja oppilaitokset tietokannasta nipun sisällön perusteella ja
  lähettää niistä luodut SMS-muistutukset viestintäpalveluun."
  [timeout? muistutettavat]
  (log/info (str "Aiotaan käsitellä" (count muistutettavat) "muistutusta."))
  (c/doseq-with-timeout
    timeout?
    [nippu muistutettavat]
    (try
      (log/info "Kyselylinkin tunnusosa:"
                (last (str/split (:kyselylinkki nippu) #"_")))
      (let [status (arvo/get-nippulinkki-status (:kyselylinkki nippu))]
        (log/info "Arvo-status:" status)
        (if (and (not (:vastattu status))
                 (c/has-time-to-answer? (:voimassa_loppupvm status)))
          (let [jaksot (tc/get-jaksot-for-nippu nippu)
                laitokset (c/get-oppilaitokset jaksot)
                body (elisa/tep-muistutus-msg-body (:kyselylinkki nippu)
                                                   laitokset)
                resp (elisa/send-sms (:lahetettynumeroon nippu) body)
                tila (get-in resp [:body
                                   :messages
                                   (keyword (:lahetettynumeroon nippu))
                                   :status])]
            (log/info "Muistutus lähetetty, vastaus" resp)
            (tc/update-nippu nippu
                             {:sms_kasittelytila [:s tila]
                              :sms_muistutuspvm [:s (str (c/local-date-now))]
                              :sms_muistutukset [:n 1]}))
          (let [kasittely-status (if (:vastattu status)
                                   (:vastattu c/kasittelytilat)
                                   (:vastausaika-loppunut-m c/kasittelytilat))]
            (log/warn "Ei voida lähettää, status" status
                      "tila" kasittely-status)
            (tc/update-nippu nippu {:sms_kasittelytila [:s kasittely-status]
                                    :sms_muistutukset  [:n 1]}))))
      (catch Exception e
        (log/error e "nipussa" nippu)))))

(defn query-muistutukset
  "Hakee nippuja tietokannasta, joilla on aika lähettää SMS-muistutus."
  []
  (ddb/query-items {:sms_muistutukset [:eq [:n 0]]
                    :sms_lahetyspvm  [:between
                                      [[:s (str (.minusDays (c/local-date-now)
                                                            10))]
                                       [:s (str (.minusDays (c/local-date-now)
                                                            5))]]]}
                   {:index "smsMuistutusIndex"}
                   (:nippu-table env)))

(defn -handleSendSMSMuistutus
  "Hakee SMS-muistutettavia nippuja tietokannasta ja lähettää viestejä."
  [_ event ^com.amazonaws.services.lambda.runtime.Context context]
  (log-caller-details-scheduled "handleSendSMSMuistutus" event context)
  (sendSmsMuistutus (c/no-time-left? context 60000) (query-muistutukset)))
