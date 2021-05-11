(ns oph.heratepalvelu.tep.emailHandler
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.external.organisaatio :as org]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.common :as c]
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

(defn- pilotti-lahetysosoite [email jaksot]
  (let [item (ddb/get-item {:organisaatio-oid [:s (:koulutuksenjarjestaja email)]}
                           (:orgwhitelist-table env))
        pilottiosoite (:pilottiosoite item)
        ohjaaja-email (:ohjaaja_email (reduce #(if (and (some? (:ohjaaja_email %1))
                                                        (= (:ohjaaja_email %1) (:ohjaaja_email %2)))
                                                 %1
                                                 (reduced nil))
                                              jaksot))]
    (if (some? ohjaaja-email)
      (if (some? pilottiosoite)
        (if (= "ohjaaja" pilottiosoite)
          ohjaaja-email
          pilottiosoite)
        (do (log/warn "Ei pilottiosoitetta organisaatiolle" (:koulutuksenjarjestaja email))
            (ddb/update-item
              {:ohjaaja_ytunnus_kj_tutkinto [:eq [:s (:ohjaaja_ytunnus_kj_tutkinto email)]]
               :niputuspvm                  [:eq [:s (:niputuspvm email)]]}
              {:update-expr      "SET #kasittelytila = :kasittelytila"
               :expr-attr-names {"#kasittelytila" "kasittelytila"}
               :expr-attr-vals  {":kasittelytila" [:s "ei-pilottiosoitetta"]}}
              (:nippu-table env))))
      (do (log/warn "Ei yksiselitteistä ohjaajan sahköpostia "
                    (:ohjaaja_ytunnus_kj_tutkinto email) ","
                    (:niputuspvm email) ","
                    (reduce #(conj %1 (:ohjaaja_email %2)) #{} jaksot))
          (ddb/update-item
            {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto email)]
             :niputuspvm                  [:s (:niputuspvm email)]}
            {:update-expr      "SET #kasittelytila = :kasittelytila"
             :expr-attr-names {"#kasittelytila" "kasittelytila"}
             :expr-attr-vals  {":kasittelytila" [:s "email-mismatch"]}}
            (:nippu-table env))))))

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
              osoite (pilotti-lahetysosoite email jaksot)]
          (when (some? osoite)
            (if (c/has-time-to-answer? (:voimassaloppupvm email))
              (try
                (let [id (:id (vp/send-email {:subject "Työpaikkaohjaajakysely - Enkät till arbetsplatshandledaren - Survey to workplace instructors"
                                              :body (vp/tyopaikkaohjaaja-html email oppilaitokset)
                                              :address osoite
                                              :sender "Opetushallitus – Utbildningsstyrelsen"}))
                      lahetyspvm (str (t/today))]
                  (ddb/update-item
                    {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto email)]
                     :niputuspvm                  [:s (:niputuspvm email)]}
                    {:update-expr     (str "SET #kasittelytila = :kasittelytila, "
                                           "#vpid = :vpid, "
                                           "#lahetyspvm = :lahetyspvm, "
                                           "#muistutukset = :muistutukset")
                     :expr-attr-names {"#kasittelytila" "kasittelytila"
                                       "#vpid" "viestintapalvelu-id"
                                       "#lahetyspvm" "lahetyspvm"
                                       "#muistutukset" "muistutukset"}
                     :expr-attr-vals  {":kasittelytila" [:s (:viestintapalvelussa c/kasittelytilat)]
                                       ":vpid" [:n id]
                                       ":lahetyspvm" [:s lahetyspvm]
                                       ":muistutukset" [:n 0]}}
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
                  {:update-expr     (str "SET #lahetystila = :lahetystila, "
                                         "#lahetyspvm = :lahetyspvm")
                   :expr-attr-names {"#lahetystila" "lahetystila"
                                     "#lahetyspvm" "lahetyspvm"}
                   :expr-attr-vals {":lahetystila" [:s (:vastausaika-loppunut c/kasittelytilat)]
                                    ":lahetyspvm" [:s (str (t/today))]}}
                  (:nippu-table env))
                (catch Exception e
                  (log/error "Virhe lähetystilan päivityksessä nipulle, jonka vastausaika umpeutunut" email)
                  (log/error e)))))))
      (when (< 60000 (.getRemainingTimeInMillis context))
        (recur (ddb/query-items {:kasittelytila [:eq [:s (:ei-lahetetty c/kasittelytilat)]]
                                 :niputuspvm    [:le [:s (str (t/today))]]}
                                {:index "niputusIndex"
                                 :limit 10}
                                (:nippu-table env)))))))
