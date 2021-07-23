(ns oph.heratepalvelu.tep.EmailMuistutusHandler
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.common :as c]
            [clojure.tools.logging :as log]
            [cheshire.core :refer [parse-string]]
            [environ.core :refer [env]])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)
           (java.time LocalDate)))

(gen-class
  :name "oph.heratepalvelu.tep.EmailMuistutusHandler"
  :methods [[^:static handleSendEmailMuistutus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn- sendAMISMuistutus [muistutettavat]
  (log/info (str "Käsitellään " (count muistutettavat)
                 " lähetettävää muistutusta."))
  (doseq [email muistutettavat]
    (let [status (arvo/get-nippulinkki-status (:kyselylinkki email))]
      (if (and (not (:vastattu status))
               (c/has-time-to-answer? (:voimassa_loppupvm status)))
        (try
          (let [id (:id (vp/send-email {:subject "Muistutus-påminnelse-reminder: Vastaa kyselyyn - svara på enkäten - answer the survey"
                                     :body (vp/tep-muistutus-html email)
                                     :address (:lahetysosoite email)
                                     :sender "Opetushallitus – Utbildningsstyrelsen"}))]
            (ddb/update-item
              {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto email)]
               :niputuspvm                  [:s (:niputuspvm email)]}
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
                                 ":muistutuspvm" [:s (str (LocalDate/now))]
                                 ":muistutukset" [:n 1]}}
              (:nippu-table env)))
          (catch AwsServiceException e
            (log/error "Muistutus " email " lähetty viestintäpalveluun, muttei päivitetty kantaan!")
            (log/error e))
          (catch Exception e
            (log/error "Virhe muistutuksen lähetyksessä!" email)
            (log/error e)))
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
                                                   (:vastausaika-loppunut-m c/kasittelytilat))]
                              ":muistutukset" [:n 1]}})
          (catch Exception e
            (log/error "Virhe lähetystilan päivityksessä herätteelle, johon on vastattu tai jonka vastausaika umpeutunut" email)
            (log/error e)))))))

(defn- query-muistukset []
  (ddb/query-items {:muistutukset [:eq [:n 0]]
                    :lahetyspvm   [:between
                                   [[:s (str
                                          (.minusDays (LocalDate/now) 5))]
                                    [:s (str
                                          (.minusDays (LocalDate/now) 10))]]]}
                   {:index "emailMuistutusIndex"
                    :limit 50}))

(defn -handleSendAMISMuistutus [this event context]
  (log-caller-details-scheduled "handleSendAMISMuistutus" event context)
  (loop [muistutettavat (query-muistukset )]
    (sendAMISMuistutus muistutettavat)
    (when (and
            (seq muistutettavat)
            (< 60000 (.getRemainingTimeInMillis context)))
      (recur (query-muistukset)))))
