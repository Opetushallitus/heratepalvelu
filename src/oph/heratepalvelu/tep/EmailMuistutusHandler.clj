(ns oph.heratepalvelu.tep.EmailMuistutusHandler
  (:require [cheshire.core :refer [parse-string]]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.external.organisaatio :as org]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.log.caller-log :refer :all])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.tep.EmailMuistutusHandler"
  :methods [[^:static handleSendEmailMuistutus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn do-jakso-query [nippu]
  (try
    (ddb/query-items {:ohjaaja_ytunnus_kj_tutkinto [:eq [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]]
                      :niputuspvm                  [:eq [:s (:niputuspvm nippu)]]}
                     {:index "niputusIndex"}
                     (:jaksotunnus-table env))
    (catch AwsServiceException e
      (log/error "Jakso-query epäonnistui nipulla" nippu)
      (log/error e))))

(defn get-oppilaitokset [jaksot]
  (try
    (seq (into #{} (map #(:nimi (org/get-organisaatio (:oppilaitos %1)))
                        jaksot)))
    (catch Exception e
      (log/error "Virhe kutsussa organisaatiopalveluun")
      (log/error e))))

(defn send-reminder-email [nippu oppilaitokset]
  (try
    (vp/send-email {:subject "Muistutus-påminnelse-reminder: Työpaikkaohjaajakysely - Enkät till arbetsplatshandledaren - Survey to workplace instructors"
                    :body (vp/tyopaikkaohjaaja-muistutus-html nippu oppilaitokset)
                    :address (:lahetysosoite nippu)
                    :sender "OPH – UBS – EDUFI"})
    (catch Exception e
      (log/error "Virhe muistutuksen lähetyksessä!" nippu)
      (log/error e))))

(defn update-item-email-sent [nippu id]
  (try
    (ddb/update-item
      {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]
       :niputuspvm                  [:s (:niputuspvm nippu)]}
      {:update-expr     (str "SET #kasittelytila = :kasittelytila, "
                             "#vpid = :vpid, "
                             "#muistutuspvm = :muistutuspvm, "
                             "#muistutukset = :muistutukset")
       :expr-attr-names {"#kasittelytila" "kasittelytila"
                         "#vpid" "viestintapalvelu-id"
                         "#muistutuspvm" "email_muistutuspvm"
                         "#muistutukset" "muistutukset"}
       :expr-attr-vals  {":kasittelytila" [:s (:viestintapalvelussa c/kasittelytilat)]
                         ":vpid" [:n id]
                         ":muistutuspvm" [:s (str (c/local-date-now))]
                         ":muistutukset" [:n 1]}}
      (:nippu-table env))
    (catch AwsServiceException e
      (log/error "Muistutus" nippu "ei päivitetty kantaan!")
      (log/error e))))

(defn update-item-cannot-answer [nippu status]
  (try
    (ddb/update-item
      {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]
       :niputuspvm                  [:s (:niputuspvm nippu)]}
      {:update-expr     (str "SET #kasittelytila = :kasittelytila, "
                             "#muistutukset = :muistutukset")
       :expr-attr-names {"#kasittelytila" "kasittelytila"
                         "#muistutukset" "muistutukset"}
       :expr-attr-vals  {":kasittelytila" [:s (if (:vastattu status)
                                                (:vastattu c/kasittelytilat)
                                                (:vastausaika-loppunut-m c/kasittelytilat))]
                         ":muistutukset" [:n 1]}}
      (:nippu-table env))
    (catch Exception e
      (log/error "Virhe lähetystilan päivityksessä nippulinkille, johon on vastattu tai jonka vastausaika umpeutunut" nippu)
      (log/error status)
      (log/error e))))

(defn sendEmailMuistutus [muistutettavat]
  (log/info "Käsitellään" (count muistutettavat) "lähetettävää muistutusta.")
  (doseq [nippu muistutettavat]
    (log/info "Kyselylinkin tunnusosa:" (last (str/split (:kyselylinkki nippu) #"_")))
    (let [status (arvo/get-nippulinkki-status (:kyselylinkki nippu))]
      (if (and (not (:vastattu status))
               (c/has-time-to-answer? (:voimassa_loppupvm status)))
        (let [jaksot (do-jakso-query nippu)
              oppilaitokset (get-oppilaitokset jaksot)
              id (:id (send-reminder-email nippu oppilaitokset))]
          (update-item-email-sent nippu id))
        (update-item-cannot-answer nippu status)))))

(defn query-muistutukset []
  (ddb/query-items {:muistutukset [:eq [:n 0]]
                    :lahetyspvm   [:between
                                   [[:s (str (.minusDays (c/local-date-now) 10))]
                                    [:s (str (.minusDays (c/local-date-now) 5))]]]}
                   {:index "emailMuistutusIndex"
                    :limit 10}
                   (:nippu-table env)))

(defn -handleSendEmailMuistutus [this event context]
  (log-caller-details-scheduled "handleSendEmailMuistutus" event context)
  (loop [muistutettavat (query-muistutukset)]
    (sendEmailMuistutus muistutettavat)
    (when (and
            (seq muistutettavat)
            (< 60000 (.getRemainingTimeInMillis context)))
      (recur (query-muistutukset)))))
