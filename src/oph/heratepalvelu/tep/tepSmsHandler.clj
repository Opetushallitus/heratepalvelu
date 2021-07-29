(ns oph.heratepalvelu.tep.tepSmsHandler
  (:require [clojure.tools.logging :as log]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.elisa :as elisa]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.organisaatio :as org]
            [oph.heratepalvelu.external.arvo :as arvo])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)
           (clojure.lang ExceptionInfo)
           (java.time LocalDate)
           (com.google.i18n.phonenumbers PhoneNumberUtil NumberParseException)))

(gen-class
  :name "oph.heratepalvelu.tep.tepSmsHandler"
  :methods [[^:static handleTepSmsSending
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn- valid-number? [number]
  (try
    (let [utilobj (PhoneNumberUtil/getInstance)
          numberobj (.parse utilobj number "FI")]
      (and (empty? (filter (fn [x] (Character/isLetter x)) number))
           (.isValidNumber utilobj numberobj)
           (= (str (.getNumberType utilobj numberobj))
              "MOBILE")))
    (catch NumberParseException e
      (log/error "PhoneNumberUtils failed to parse phonenumber " number)
      (log/error e)
      false)))

(defn- update-status-to-db [status puhelinnumero nippu]
  (let [ohjaaja_ytunnus_kj_tutkinto (:ohjaaja_ytunnus_kj_tutkinto nippu)
        niputuspvm                  (:niputuspvm nippu)]
    (try
      (ddb/update-item
        {:ohjaaja_ytunnus_kj_tutkinto [:s ohjaaja_ytunnus_kj_tutkinto]
         :niputuspvm                  [:s niputuspvm]}
        {:update-expr     (str "SET #sms_kasittelytila = :sms_kasittelytila, "
                               "#sms_lahetyspvm = :sms_lahetyspvm, "
                               "#sms_muistutukset = :sms_muistutukset, "
                               "#lahetettynumeroon = :lahetettynumeroon")
         :expr-attr-names {"#sms_kasittelytila" "sms_kasittelytila"
                           "#sms_lahetyspvm" "sms_lahetyspvm"
                           "#sms_muistutukset" "sms_muistutukset"
                           "#lahetettynumeroon" "lahetettynumeroon"}
         :expr-attr-vals  {":sms_kasittelytila" [:s status]
                           ":sms_lahetyspvm"    [:s (str (LocalDate/now))]
                           ":sms_muistutukset"  [:n 0]
                           ":lahetettynumeroon" [:s puhelinnumero]}}
        (:nippu-table env))
      (catch Exception e
        (log/error (str "Error in update-status-to-db for " ohjaaja_ytunnus_kj_tutkinto " , " niputuspvm " , " status))
        (throw e)))))

(defn- ohjaaja-puhnro [nippu jaksot]
  (let [number (:ohjaaja_puhelinnumero (reduce #(if (some? (:ohjaaja_puhelinnumero %1))
                                                 (if (some? (:ohjaaja_puhelinnumero %2))
                                                   (if (= (:ohjaaja_puhelinnumero %1) (:ohjaaja_puhelinnumero %2))
                                                     %1
                                                     (reduced nil))
                                                   %1)
                                                 %2)
                                              jaksot))]
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
            (arvo/patch-nippulinkki-metadata (:kyselylinkki nippu) (:ei-yhteystietoja c/kasittelytilat)))
          nil))
      (let [numerot (reduce #(if (some? (:ohjaaja_puhelinnumero %2))
                               (conj %1 (:ohjaaja_puhelinnumero %2))
                               %1) #{} jaksot)]
        (log/warn "Ei yksiselitteistä ohjaajan puhelinnumeroa "
                  (:ohjaaja_ytunnus_kj_tutkinto nippu) ","
                  (:niputuspvm nippu) "," numerot)
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
            (arvo/patch-nippulinkki-metadata (:kyselylinkki nippu) (:ei-yhteystietoja c/kasittelytilat)))
          nil))))

(defn -handleTepSmsSending [this event context]
  (log-caller-details-scheduled "tepSmsHandler" event context)
  (loop [lahetettavat (ddb/query-items {:sms_kasittelytila [:eq [:s (:ei-lahetetty c/kasittelytilat)]]
                                        :niputuspvm    [:le [:s (str (LocalDate/now))]]}
                                       {:index "smsIndex"
                                        :limit 20}
                                       (:nippu-table env))]
    (log/info "Käsitellään " (count lahetettavat) " lähetettävää viestiä.")
    (when (seq lahetettavat)
      (doseq [nippu lahetettavat]
        (when-not (= (:ei-niputettu c/kasittelytilat) (:kasittelytila nippu))
          (if (c/has-time-to-answer? (:voimassaloppupvm nippu))
            (let [jaksot (ddb/query-items {:ohjaaja_ytunnus_kj_tutkinto [:eq [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]]
                                           :niputuspvm                  [:eq [:s (:niputuspvm nippu)]]}
                                          {:index "niputusIndex"}
                                          (:jaksotunnus-table env))
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
                        status (get-in resp [:body :messages (keyword puhelinnumero) :status])]
                    (update-status-to-db (:status status) (or (:converted status) puhelinnumero) nippu)
                    (when (or (= status "CREATED") (= status "mock-lahetys"))
                      (arvo/patch-nippulinkki-metadata (:kyselylinkki nippu) (:success c/kasittelytilat))))
                  (catch AwsServiceException e
                    (log/error (str "SMS-viestin lähetysvaiheen kantapäivityksessä tapahtui virhe!"))
                    (log/error e)
                    (throw e))
                  (catch ExceptionInfo e
                    (if (and
                          (> 399 (:status (ex-data e)))
                          (< 500 (:status (ex-data e))))
                      (do
                        (log/error "Client error while sending sms to number " puhelinnumero)
                        (log/error e)
                        (throw e))
                      (do
                        (log/error "Server error while sending sms to number " puhelinnumero)
                        (log/error e)
                        (throw e))))
                  (catch Exception e
                    (log/error "Unhandled exception " e)
                    (throw e)))))
            (try
              (ddb/update-item
                {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]
                 :niputuspvm                  [:s (:niputuspvm nippu)]}
                {:update-expr     (str "SET #sms_kasittelytila = :sms_kasittelytila, "
                                       "#sms_lahetyspvm = :sms_lahetyspvm")
                 :expr-attr-names {"#sms_kasittelytila" "sms_kasittelytila"
                                   "#sms_lahetyspvm" "sms_lahetyspvm"}
                 :expr-attr-vals {":sms_kasittelytila" [:s (:vastausaika-loppunut c/kasittelytilat)]
                                  ":sms_lahetyspvm" [:s (str (LocalDate/now))]}}
                (:nippu-table env))
              (catch Exception e
                (log/error "Virhe sms-lähetystilan päivityksessä nipulle, jonka vastausaika umpeutunut" nippu)
                (log/error e))))))
      (when (< 60000 (.getRemainingTimeInMillis context))
        (recur (ddb/query-items {:sms_kasittelytila [:eq [:s (:ei-lahetetty c/kasittelytilat)]]
                                 :niputuspvm    [:le [:s (str (LocalDate/now))]]}
                                {:index "smsIndex"
                                 :limit 10}
                                (:nippu-table env)))))))
