(ns oph.heratepalvelu.amis.AMISSMSMuistutusHandler
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.amis.AMISCommon :as ac]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.log.caller-log :refer
             [log-caller-details-scheduled]])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)
           (com.amazonaws.services.lambda.runtime.events ScheduledEvent)
           (com.amazonaws.services.lambda.runtime Context)))

(gen-class
  :name "oph.heratepalvelu.amis.AMISSMSMuistutusHandler"
  :methods
  [[^:static handleSendAMISSMSMuistutus
    [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
     com.amazonaws.services.lambda.runtime.Context] void]])

(defn send-sms-muistutus! [herate]
  (let [oppilaitos (org/get-organisaatio (:oppilaitos herate))
        message (elisa/amis-muistutus-msg-body
                  (:kyselylinkki herate) (:fi (:nimi oppilaitos)))]
    (elisa/send-sms (:puhelinnumero herate) message)))
  
(defn update-muistutus-sent! [herate success-state]
  (ac/update-herate herate {:sms-lahetystila [:s success-state]}))

(defn update-muistutus-not-sent! [herate status]
  (let [tila ((if (:vastattu status) :vastattu :vastausaika-loppunut-m)
              c/kasittelytilat)]
    (ac/update-herate herate {:sms-lahetystila tila})))

(defn maybe-send-sms-muistutus! [herate success-state]
  (let [status (arvo/get-kyselylinkki-status (:kyselylinkki herate))
        still-open? (c/has-time-to-answer? (:voimassa_loppupvm status))]
    (log/info "Heräte" herate "arvo-status" status)
    (if (and (not (:vastattu status)) still-open?)
      (do (send-sms-muistutus! herate)
          (update-muistutus-sent! herate success-state))
      (update-muistutus-not-sent! herate status still-open?))))

(defn send-sms-muistutukset! [end-condition heratteet success-state]
  (log/info "Aiotaan lähettää" (count heratteet) "SMS-muistutusta.")
  (c/doseq-with-timeout
    end-condition
    [herate heratteet]
    (try (maybe-send-sms-muistutus! herate success-state)
         (catch Exception e (log/error e "herätteessä" herate)))))

(defn query-first-muistutukset []
  (ddb/query-items-with-expression
    "#smstila = :tila and alkupvm <= :ylaraja and alkupvm >= :alaraja"
    {:index "smsIndex"
     :expr-attr-names {"#smstila" "sms-lahetystila"}
     :expr-attr-vals {":tila" "CREATED"
                      ":alaraja" (str (.minusDays (c/local-date-now) 10))
                      ":ylaraja" (str (.minusDays (c/local-date-now) 5))}}
    (:herate-table env)))

(defn query-second-muistutukset []
  (ddb/query-items-with-expression
    "#smstila = :tila and alkupvm <= :ylaraja and alkupvm >= :alaraja"
    {:index "smsIndex"
     :expr-attr-names {"#smstila" "sms-lahetystila"}
     :expr-attr-vals {":tila" "muistutus-1-lahetetty"
                      ":alaraja" (str (.minusDays (c/local-date-now) 15))
                      ":ylaraja" (str (.minusDays (c/local-date-now) 10))}}
    (:herate-table env)))

(defn -handleSendAMISSMSMuistutus
  "Hakee SMS-muistutettavia nippuja tietokannasta ja lähettää viestejä."
  [_ event ^Context context]
  (log-caller-details-scheduled "handleSendAMISSMSMuistutus" event context)
  (let [timeout (c/no-time-left? context 60000)]
    (send-sms-muistutukset!
      timeout (query-first-muistutukset) "muistutus-1-lahetetty")
    (send-sms-muistutukset!
      timeout (query-second-muistutukset) "muistutus-2-lahetetty")))
