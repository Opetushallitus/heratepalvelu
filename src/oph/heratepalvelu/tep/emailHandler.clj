(ns oph.heratepalvelu.tep.emailHandler
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.external.organisaatio :as org]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.aws-sqs :as sqs]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [clj-time.core :as t]
            [cheshire.core :refer [parse-string]])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.tep.emailHandler"
  :methods [[^:static handleSendTEPEmails
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn- lahetysosoite [email jaksot]
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
      (do (log/warn "Ei yksiselitteistä ohjaajan sahköpostia "
                    (:ohjaaja_ytunnus_kj_tutkinto email) ","
                    (:niputuspvm email) ","
                    (reduce #(conj %1 (:ohjaaja_email %2)) #{} jaksot))
          (ddb/update-item
            {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto email)]
             :niputuspvm                  [:s (:niputuspvm email)]}
            {:update-expr      "SET #kasittelytila = :kasittelytila"
             :expr-attr-names {"#kasittelytila" "kasittelytila"}
             :expr-attr-vals  {":kasittelytila" [:s (:email-mismatch c/kasittelytilat)]}}
            (:nippu-table env))
          nil))))

(defn- sms-lahetysosoite [email jaksot]
  (let [phone (:ohjaaja_puhelinnumero (reduce #(if (and (some? (:ohjaaja_puhelinnumero %1))
                                                         (= (:ohjaaja_puhelinnumero %1)
                                                            (:ohjaaja_puhelinnumero %2)))
                                                  %1
                                                  (reduced nil))
                                               jaksot))]
    (if (some? phone)
      phone
      (do (log/warn "Ei yksiselitteistä ohjaajan puhelinnumeroa "
                    (:ohjaaja_ytunnus_kj_tutkinto email) ","
                    (:niputuspvm email) ","
                    (reduce #(conj %1 (:ohjaaja_puhelinnumero %2)) #{} jaksot))
          (ddb/update-item
            {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto email)]
             :niputuspvm                  [:s (:niputuspvm email)]}
            {:update-expr     (str "SET #sms-kasittelytila = :sms-kasittelytila")
             :expr-attr-names {"#sms-kasittelytila" "sms_kasittelytila"}
             :expr-attr-vals {":sms-kasittelytila" (:phone-mismatch c/kasittelytilat)}}
            (:nippu-table env))
          nil))))

(defn -handleSendTEPEmails [this event context]
  (log-caller-details-scheduled "handleSendTEPEmails" event context)
  (loop [lahetettavat (ddb/query-items {:kasittelytila [:eq [:s (:ei-lahetetty c/kasittelytilat)]]
                                        :niputuspvm    [:le [:s (str (t/today))]]}
                                       {:index "niputusIndex"
                                        :limit 20}
                                       (:nippu-table env))]
    (log/info "Käsitellään " (count lahetettavat) " lähetettävää viestiä.")
    (when (seq lahetettavat)
      (doseq [email lahetettavat]
        (let [jaksot (ddb/query-items {:ohjaaja_ytunnus_kj_tutkinto [:eq [:s (:ohjaaja_ytunnus_kj_tutkinto email)]]
                                       :niputuspvm                  [:eq [:s (:niputuspvm email)]]}
                                      {:index "niputusIndex"}
                                      (:jaksotunnus-table env))
              oppilaitokset (seq (into #{}
                                       (map
                                         #(:nimi (org/get-organisaatio (:oppilaitos %1)))
                                         jaksot)))
              osoite (lahetysosoite email jaksot)
              puhelinnumero (sms-lahetysosoite email jaksot)
              sms-kasittelytila (:sms_kasittelytila email)]
          (if (c/has-time-to-answer? (:voimassaloppupvm email))
            (when (some? osoite)
              (try
                (let [id (:id (vp/send-email {:subject "Työpaikkaohjaajakysely - Enkät till arbetsplatshandledaren - Survey to workplace instructors"
                                              :body (vp/tyopaikkaohjaaja-html
                                                      email
                                                      oppilaitokset)
                                              :address osoite
                                              :sender "Opetushallitus – Utbildningsstyrelsen"}))
                      lahetyspvm (str (t/today))]
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
                    (:nippu-table env)))
                (catch AwsServiceException e
                  (log/error "Viesti " email " lähetty viestintäpalveluun, muttei päivitetty kantaan!")
                  (log/error e))
                (catch Exception e
                  (log/error "Virhe viestin lähetyksessä!" email)
                  (log/error e)))
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
            (when (and (some? puhelinnumero)
                       (or (nil? sms-kasittelytila)
                           (= sms-kasittelytila (:ei-lahetetty c/kasittelytilat))))
              (try
                (sqs/send-tep-sms-sqs-message (sqs/build-sms-sqs-message
                                                oppilaitokset
                                                puhelinnumero
                                                (:ohjaaja_ytunnus_kj_tutkinto email)
                                                (:niputuspvm email)))
                (ddb/update-item
                  {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto email)]
                   :niputuspvm                  [:s (:niputuspvm email)]}
                  {:update-expr     (str "SET #sms-kasittelytila = :sms-kasittelytila")
                   :expr-attr-names {"#sms-kasittelytila" "sms_kasittelytila"}
                   :expr-attr-vals {":sms-kasittelytila" [:s (:queued c/kasittelytilat)]}}
                  (:nippu-table env))
              (catch AwsServiceException e
                (log/error (str "SMS-viestin tilan päivityksessä tapahtunut virhe! Numero: " puhelinnumero))
                (log/error e))
              (catch Exception e
                (log/error e))))))))
      (when (< 60000 (.getRemainingTimeInMillis context))
        (recur (ddb/query-items {:kasittelytila [:eq [:s (:ei-lahetetty c/kasittelytilat)]]
                                 :niputuspvm    [:le [:s (str (t/today))]]}
                                {:index "niputusIndex"
                                 :limit 10}
                                (:nippu-table env))))))
