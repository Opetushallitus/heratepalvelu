(ns oph.heratepalvelu.tep.EmailMuistutusHandler
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.external.organisaatio :as org]
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

(defn- sendEmailMuistutus [muistutettavat]
  (log/info (str "Käsitellään " (count muistutettavat)
                 " lähetettävää muistutusta."))
  (doseq [nippu muistutettavat]
    (let [status (arvo/get-nippulinkki-status (:kyselylinkki nippu))]
      (if (and (not (:vastattu status))
               (c/has-time-to-answer? (:voimassa_loppupvm status)))
        (try
          (let [jaksot (ddb/query-items {:ohjaaja_ytunnus_kj_tutkinto [:eq [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]]
                                                   :niputuspvm                  [:eq [:s (:niputuspvm nippu)]]}
                                                  {:index "niputusIndex"}
                                                  (:jaksotunnus-table env))
                oppilaitokset (seq (into #{}
                                         (map
                                           #(:nimi (org/get-organisaatio (:oppilaitos %1)))
                                           jaksot)))
                id (:id (vp/send-email {:subject "Muistutus-påminnelse-reminder: Työpaikkaohjaajakysely - Enkät till arbetsplatshandledaren - Survey to workplace instructors"
                                     :body (vp/tyopaikkaohjaaja-html nippu oppilaitokset)
                                     :address (:lahetysosoite nippu)
                                     :sender "OPH – UBS"}))]
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
                                 ":muistutuspvm" [:s (str (LocalDate/now))]
                                 ":muistutukset" [:n 1]}}
              (:nippu-table env)))
          (catch AwsServiceException e
            (log/error "Muistutus " nippu " lähetty viestintäpalveluun, muttei päivitetty kantaan!")
            (log/error e))
          (catch Exception e
            (log/error "Virhe muistutuksen lähetyksessä!" nippu)
            (log/error e)))
        (try
          (ddb/update-item
            {:toimija_oppija [:s (:toimija_oppija nippu)]
             :tyyppi_kausi   [:s (:tyyppi_kausi nippu)]}
            {:update-expr     (str "SET #lahetystila = :lahetystila, "
                                   "#muistutukset = :muistutukset")
             :expr-attr-names {"#lahetystila" "lahetystila"
                               "#muistutukset" "muistutukset"}
             :expr-attr-vals {":lahetystila" [:s (if (:vastattu status)
                                                   (:vastattu c/kasittelytilat)
                                                   (:vastausaika-loppunut-m c/kasittelytilat))]
                              ":muistutukset" [:n 1]}})
          (catch Exception e
            (log/error "Virhe lähetystilan päivityksessä nippulinkille, johon on vastattu tai jonka vastausaika umpeutunut" nippu)
            (log/error e)))))))

(defn- query-muistukset []
  (ddb/query-items {:muistutukset [:eq [:n 0]]
                    :lahetyspvm   [:eq [:s (str (.minusDays (LocalDate/now) 5))]]}
                   {:index "emailMuistutusIndex"
                    :limit 50}))

(defn -handleSendEmailMuistutus [this event context]
  (log-caller-details-scheduled "handleSendEmailMuistutus" event context)
  (loop [muistutettavat (query-muistukset )]
    (sendEmailMuistutus muistutettavat)
    (when (and
            (seq muistutettavat)
            (< 60000 (.getRemainingTimeInMillis context)))
      (recur (query-muistukset)))))
