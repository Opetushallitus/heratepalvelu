(ns oph.heratepalvelu.tep.SMSMuistutusHandler
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.common :as c]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.external.elisa :as elisa]
            [environ.core :refer [env]])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)
           (java.time LocalDate)))

(gen-class
  :name "oph.heratepalvelu.tep.SMSMuistutusHandler"
  :methods [[^:static handleSendSMSMuistutus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn- sendAMISMuistutus [muistutettavat]
  (log/info (str "Käsitellään " (count muistutettavat) " muistutusta."))
  (doseq [nippu muistutettavat]
    (let [status (arvo/get-nippulinkki-status (:kyselylinkki nippu))
          ohjaaja_ytunnus_kj_tutkinto (:ohjaaja_ytunnus_kj_tutkinto nippu)
          niputuspvm (:niputuspvm nippu)]
      (if (and (not (:vastattu status))
               (c/has-time-to-answer? (:voimassa_loppupvm status)))
        (try
          (let
            (ddb/update-item
              {:ohjaaja_ytunnus_kj_tutkinto [:s ohjaaja_ytunnus_kj_tutkinto]
               :niputuspvm                  [:s niputuspvm]}
              {:update-expr     (str "SET #sms_kasittelytila = :sms_kasittelytila, "
                                     "#sms_muistutuspvm = :sms_muistutuspvm, "
                                     "#sms_muistutukset = :sms_muistutukset")
               :expr-attr-names {"#sms_kasittelytila" "sms_kasittelytila"
                                 "#sms_muistutuspvm"  "sms_muistutuspvm"
                                 "#sms_muistutukset"  "sms_muistutukset"}
               :expr-attr-vals  {":sms_kasittelytila" [:s status]
                                 ":sms_muistutuspvm"  [:s (str (LocalDate/now))]
                                 ":sms_muistutukset"  [:n 1]}}
              (:nippu-table env)))
          (catch AwsServiceException e
            (log/error "Muistutus " nippu " lähetty viestintäpalveluun, muttei päivitetty kantaan!")
            (log/error e))
          (catch Exception e
            (log/error "Virhe muistutuksen lähetyksessä!" nippu)
            (log/error e)))
        (try
          (ddb/update-item
            {:ohjaaja_ytunnus_kj_tutkinto [:s ohjaaja_ytunnus_kj_tutkinto]
             :niputuspvm                  [:s niputuspvm]}
            {:update-expr     (str "SET #sms_lahetystila = :sms_lahetystila, "
                                   "#sms_muistutukset = :sms_muistutukset")
             :expr-attr-names {"#sms_lahetystila" "sms_lahetystila"
                               "#sms_muistutukset" "sms_muistutukset"}
             :expr-attr-vals {":sms_lahetystila" [:s (if (:vastattu status)
                                                   (:vastattu c/kasittelytilat)
                                                   (:vastausaika-loppunut-m c/kasittelytilat))]
                              ":sms_muistutukset" [:n 1]}})
          (catch Exception e
            (log/error "Virhe lähetystilan päivityksessä herätteelle, johon on vastattu tai jonka vastausaika umpeutunut" nippu)
            (log/error e)))))))

(defn- query-muistukset []
  (ddb/query-items {:sms_muistutukset [:eq [:n 0]]
                    :sms_lahetyspvm  [:between
                                  [[:s (str
                                         (.minusDays (LocalDate/now) 5))]
                                   [:s (str
                                         (.minusDays (LocalDate/now) 10))]]]}
                   {:index "smsMuistutusIndex"
                    :limit 50}))

(defn -handleSendAMISMuistutus [this event context]
  (log-caller-details-scheduled "handleSendAMISMuistutus" event context)
  (loop [muistutettavat (query-muistukset)]
    (sendAMISMuistutus muistutettavat)
    (when (and
            (seq muistutettavat)
            (< 60000 (.getRemainingTimeInMillis context)))
      (recur (query-muistukset)))))
