(ns oph.heratepalvelu.amis.AMISMuistutusHandler
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.log.caller-log :refer :all])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.amis.AMISMuistutusHandler"
  :methods [[^:static handleSendAMISMuistutus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn update-after-send [email n id]
  (try
    (ddb/update-item
      {:toimija_oppija [:s (:toimija_oppija email)]
       :tyyppi_kausi   [:s (:tyyppi_kausi email)]}
      {:update-expr    (str "SET #muistutukset = :muistutukset, "
                            "#vpid = :vpid, "
                            "#lahetystila = :lahetystila, "
                            "#muistutuspvm = :muistutuspvm")
       :expr-attr-names {"#muistutukset" "muistutukset"
                         "#vpid" "viestintapalvelu-id"
                         "#lahetystila" "lahetystila"
                         "#muistutuspvm" (str n ".-muistutus-lahetetty")}
       :expr-attr-vals  {":muistutukset" [:n n]
                         ":vpid" [:n id]
                         ":lahetystila" [:s (:viestintapalvelussa
                                              c/kasittelytilat)]
                         ":muistutuspvm" [:s (str (c/local-date-now))]}})
    (catch AwsServiceException e
      (log/error "Muistutus"
                 email
                 "lähetty viestintäpalveluun, muttei päivitetty kantaan!")
      (log/error e))))

(defn update-when-not-sent [email n status]
  (try
    (ddb/update-item
      {:toimija_oppija [:s (:toimija_oppija email)]
       :tyyppi_kausi   [:s (:tyyppi_kausi email)]}
      {:update-expr     (str "SET #lahetystila = :lahetystila, "
                             "#muistutukset = :muistutukset")
       :expr-attr-names {"#lahetystila" "lahetystila"
                         "#muistutukset" "muistutukset"}
       :expr-attr-vals {":lahetystila" [:s (if (:vastattu status)
                                             (:vastattu c/kasittelytilat)
                                             (:vastausaika-loppunut-m
                                               c/kasittelytilat))]
                        ":muistutukset" [:n n]}})
    (catch Exception e
      (log/error "Virhe lähetystilan päivityksessä herätteelle, johon on"
                 "vastattu tai jonka vastausaika umpeutunut"
                 email)
      (log/error e))))

(defn send-reminder-email [email]
  (vp/send-email
    {:subject (str "Muistutus-påminnelse-reminder: "
                   "Vastaa kyselyyn - svara på enkäten - answer the survey")
     :body (vp/amismuistutus-html email)
     :address (:sahkoposti email)
     :sender "Opetushallitus – Utbildningsstyrelsen – EDUFI"}))

(defn sendAMISMuistutus [muistutettavat n]
  (log/info (str "Käsitellään " (count muistutettavat)
                 " lähetettävää " n ". muistutusta."))
  (doseq [email muistutettavat]
    (let [status (arvo/get-kyselylinkki-status (:kyselylinkki email))]
      (if (and (not (:vastattu status))
               (c/has-time-to-answer? (:voimassa_loppupvm status)))
        (try
          (let [id (:id (send-reminder-email email))]
            (update-after-send email n id))
          (catch Exception e
            (log/error "Virhe muistutuksen lähetyksessä!" email)
            (log/error e)))
        (update-when-not-sent email n status)))))

(defn query-muistutukset [n]
  (ddb/query-items {:muistutukset [:eq [:n (- n 1)]]
                    :lahetyspvm  [:between
                                  [[:s (str (.minusDays (c/local-date-now)
                                                        (- (* 5 (+ n 1)) 1)))]
                                   [:s (str (.minusDays (c/local-date-now)
                                                        (* 5 n)))]]]}
                   {:index "muistutusIndex"
                    :limit 50}))

(defn -handleSendAMISMuistutus [this event context]
  (log-caller-details-scheduled "handleSendAMISMuistutus" event context)
  (loop [muistutettavat1 (query-muistutukset 1)
         muistutettavat2 (query-muistutukset 2)]
    (sendAMISMuistutus muistutettavat1 1)
    (sendAMISMuistutus muistutettavat2 2)
    (when (and
            (or (seq muistutettavat1) (seq muistutettavat2))
            (< 60000 (.getRemainingTimeInMillis context)))
      (recur (query-muistutukset 1)
             (query-muistutukset 2)))))
