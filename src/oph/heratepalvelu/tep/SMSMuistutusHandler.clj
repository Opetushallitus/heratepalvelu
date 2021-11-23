(ns oph.heratepalvelu.tep.SMSMuistutusHandler
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.external.elisa :as elisa]
            [oph.heratepalvelu.external.organisaatio :as org]
            [oph.heratepalvelu.log.caller-log :refer :all])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)
           (java.time LocalDate)))

(gen-class
  :name "oph.heratepalvelu.tep.SMSMuistutusHandler"
  :methods [[^:static handleSendSMSMuistutus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn- sendSmsMuistutus [muistutettavat]
  (log/info (str "Käsitellään " (count muistutettavat) " muistutusta."))
  (doseq [nippu muistutettavat]
    (log/info "Kyselylinkin tunnus-osa:" (last (str/split (:kyselylinkki nippu) "_")))
    (let [status (arvo/get-nippulinkki-status (:kyselylinkki nippu))
          ohjaaja_ytunnus_kj_tutkinto (:ohjaaja_ytunnus_kj_tutkinto nippu)
          niputuspvm (:niputuspvm nippu)]
      (if (and (not (:vastattu status))
               (c/has-time-to-answer? (:voimassa_loppupvm status)))
        (try
          (let
            [jaksot (ddb/query-items {:ohjaaja_ytunnus_kj_tutkinto [:eq [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]]
                                      :niputuspvm                  [:eq [:s (:niputuspvm nippu)]]}
                                     {:index "niputusIndex"}
                                     (:jaksotunnus-table env))
             oppilaitokset (seq (into #{}
                                      (map
                                        #(:nimi (org/get-organisaatio (:oppilaitos %1)))
                                        jaksot)))
             body (elisa/muistutus-msg-body (:kyselylinkki nippu) oppilaitokset)
             resp (elisa/send-tep-sms (:lahetettynumeroon nippu) body)
             status (get-in resp [:body :messages (keyword (:lahetettynumeroon nippu)) :status])]
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
            {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]
             :niputuspvm   [:s (:niputuspvm nippu)]}
            {:update-expr     (str "SET #sms_kasittelytila = :sms_kasittelytila, "
                                   "#sms_muistutukset = :sms_muistutukset")
             :expr-attr-names {"#sms_kasittelytila" "sms_kasittelytila"
                               "#sms_muistutukset" "sms_muistutukset"}
             :expr-attr-vals {":sms_kasittelytila" [:s (if (:vastattu status)
                                                   (:vastattu c/kasittelytilat)
                                                   (:vastausaika-loppunut-m c/kasittelytilat))]
                              ":sms_muistutukset" [:n 1]}}

            (:nippu-table env))
          (catch Exception e
            (log/error "Virhe lähetystilan päivityksessä herätteelle, johon on vastattu tai jonka vastausaika umpeutunut" nippu)
            (log/error e)))))))

(defn- query-muistukset []
  (ddb/query-items {:sms_muistutukset [:eq [:n 0]]
                    :sms_lahetyspvm  [:between
                                      [[:s (str (.minusDays (LocalDate/now) 10))]
                                       [:s (str (.minusDays (LocalDate/now) 5))]]]}
                   {:index "smsMuistutusIndex"
                    :limit 10}
                   (:nippu-table env)))

(defn -handleSendSMSMuistutus [this event context]
  (log-caller-details-scheduled "handleSendSMSMuistutus" event context)
  (loop [muistutettavat (query-muistukset)]
    (sendSmsMuistutus muistutettavat)
    (when (and
            (seq muistutettavat)
            (< 60000 (.getRemainingTimeInMillis context)))
      (recur (query-muistukset)))))
