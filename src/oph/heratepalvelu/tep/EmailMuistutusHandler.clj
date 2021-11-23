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
    (log/info "Kyselylinkin tunnusosa:" (last (str/split (:kyselylinkki nippu) #"_")))
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
                                     :body (vp/tyopaikkaohjaaja-muistutus-html nippu oppilaitokset)
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
            {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]
             :niputuspvm   [:s (:niputuspvm nippu)]}
            {:update-expr     (str "SET #kasittelytila = :kasittelytila, "
                                   "#muistutukset = :muistutukset")
             :expr-attr-names {"#kasittelytila" "kasittelytila"
                               "#muistutukset" "muistutukset"}
             :expr-attr-vals {":kasittelytila" [:s (if (:vastattu status)
                                                   (:vastattu c/kasittelytilat)
                                                   (:vastausaika-loppunut-m c/kasittelytilat))]
                              ":muistutukset" [:n 1]}}
            (:nippu-table env))
          (catch Exception e
            (log/error "Virhe lähetystilan päivityksessä nippulinkille, johon on vastattu tai jonka vastausaika umpeutunut" nippu)
            (log/error status)
            (log/error e)))))))

(defn- query-muistukset []
  (ddb/query-items {:muistutukset [:eq [:n 0]]
                    :lahetyspvm   [:between
                                   [[:s (str (.minusDays (LocalDate/now) 10))]
                                    [:s (str (.minusDays (LocalDate/now) 5))]]]}
                   {:index "emailMuistutusIndex"
                    :limit 10}
                   (:nippu-table env)))

(defn -handleSendEmailMuistutus [this event context]
  (log-caller-details-scheduled "handleSendEmailMuistutus" event context)
  (loop [muistutettavat (query-muistukset )]
    (sendEmailMuistutus muistutettavat)
    (when (and
            (seq muistutettavat)
            (< 60000 (.getRemainingTimeInMillis context)))
      (recur (query-muistukset)))))
