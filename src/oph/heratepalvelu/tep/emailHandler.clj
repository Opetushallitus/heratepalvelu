(ns oph.heratepalvelu.tep.emailHandler
  (:require [cheshire.core :refer [parse-string]]
            [clj-time.core :as t]
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

(gen-class
  :name "oph.heratepalvelu.tep.emailHandler"
  :methods [[^:static handleSendTEPEmails
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn bad-phone? [nippu]
  (or (= (:phone-mismatch c/kasittelytilat) (:sms_kasittelytila nippu))
      (= (:no-phone c/kasittelytilat) (:sms_kasittelytila nippu))
      (= (:phone-invalid c/kasittelytilat) (:sms_kasittelytila nippu))))

(defn lahetysosoite-update-item [nippu osoitteet]
  (ddb/update-item
    {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]
     :niputuspvm                  [:s (:niputuspvm nippu)]}
    {:update-expr      "SET #kasittelytila = :kasittelytila"
     :expr-attr-names {"#kasittelytila" "kasittelytila"}
     :expr-attr-vals  {":kasittelytila" [:s (if (empty? osoitteet)
                                              (:no-email c/kasittelytilat)
                                              (:email-mismatch c/kasittelytilat))]}}
    (:nippu-table env)))

(defn lahetysosoite [nippu jaksot]
  (let [ohjaaja-email (:ohjaaja_email (reduce #(if (some? (:ohjaaja_email %1))
                                                 (if (some? (:ohjaaja_email %2))
                                                   (if (= (:ohjaaja_email %1) (:ohjaaja_email %2))
                                                     %1
                                                     (reduced nil))
                                                   %1)
                                                 %2)
                                              jaksot))]
    (if (some? ohjaaja-email)
      ohjaaja-email
      (let [osoitteet (reduce
                        #(if (some? (:ohjaaja_email %2))
                          (conj %1 (:ohjaaja_email %2))
                          %1)
                        #{} jaksot)]
        (log/warn "Ei yksiselitteistä ohjaajan sahköpostia "
                    (:ohjaaja_ytunnus_kj_tutkinto nippu) ","
                    (:niputuspvm nippu) "," osoitteet)
        (lahetysosoite-update-item nippu osoitteet)
        (when (bad-phone? nippu)
          (arvo/patch-nippulinkki (:kyselylinkki nippu) {:tila (:ei-yhteystietoja c/kasittelytilat)}))
        nil))))

(defn do-nippu-query []
  (ddb/query-items {:kasittelytila [:eq [:s (:ei-lahetetty c/kasittelytilat)]]
                    :niputuspvm    [:le [:s (str (t/today))]]}
                   {:index "niputusIndex"
                    :limit 20}
                   (:nippu-table env)))

(defn email-sent-update-item [email id lahetyspvm osoite]
  (try
    (ddb/update-item
      {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto email)]
       :niputuspvm                  [:s (:niputuspvm email)]}
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
      (log/error "Viesti" email "ei päivitetty kantaan!")
      (log/error e))))

(defn send-survey-email [email oppilaitokset osoite]
  (try
    (vp/send-email {:subject "Työpaikkaohjaajakysely - Enkät till arbetsplatshandledaren - Survey to workplace instructors"
                    :body (vp/tyopaikkaohjaaja-html email oppilaitokset)
                    :address osoite
                    :sender "OPH – UBS – EDUFI"})
    (catch Exception e
      (log/error "Virhe viestin lähetyksessä!" email)
      (log/error e))))

(defn no-time-to-answer-update-item [email]
  (try
    (ddb/update-item
      {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto email)]
       :niputuspvm                  [:s (:niputuspvm email)]}
      {:update-expr     (str "SET #kasittelytila = :kasittelytila, "
                             "#lahetyspvm = :lahetyspvm")
       :expr-attr-names {"#kasittelytila" "kasittelytila"
                         "#lahetyspvm" "lahetyspvm"}
       :expr-attr-vals {":kasittelytila" [:s (:vastausaika-loppunut c/kasittelytilat)]
                        ":lahetyspvm" [:s (str (t/today))]}}
      (:nippu-table env))
    (catch Exception e
      (log/error "Virhe lähetystilan päivityksessä nipulle, jonka vastausaika umpeutunut" email)
      (log/error e))))

(defn do-jakso-query [email]
  (ddb/query-items {:ohjaaja_ytunnus_kj_tutkinto [:eq [:s (:ohjaaja_ytunnus_kj_tutkinto email)]]
                    :niputuspvm                  [:eq [:s (:niputuspvm email)]]}
                   {:index "niputusIndex"}
                   (:jaksotunnus-table env)))

(defn get-oppilaitokset [jaksot]
  (try
    (seq (into #{} (map #(:nimi (org/get-organisaatio (:oppilaitos %1)))
                        jaksot)))
    (catch Exception e
      (log/error "Virhe kutsussa organisaatiopalveluun")
      (log/error e))))

(defn -handleSendTEPEmails [this event context]
  (log-caller-details-scheduled "handleSendTEPEmails" event context)
  (loop [lahetettavat (do-nippu-query)]
    (log/info "Käsitellään " (count lahetettavat) " lähetettävää viestiä.")
    (when (seq lahetettavat)
      (doseq [email lahetettavat]
        (let [jaksot (do-jakso-query email)
              oppilaitokset (get-oppilaitokset jaksot)
              osoite (lahetysosoite email jaksot)]
          (if (c/has-time-to-answer? (:voimassaloppupvm email))
            (when (some? osoite)
              (let [id (:id (send-survey-email email oppilaitokset osoite))
                    lahetyspvm (str (t/today))]
                (email-sent-update-item email id lahetyspvm osoite)))
            (no-time-to-answer-update-item email))))
      (when (< 60000 (.getRemainingTimeInMillis context))
        (recur (do-nippu-query))))))
