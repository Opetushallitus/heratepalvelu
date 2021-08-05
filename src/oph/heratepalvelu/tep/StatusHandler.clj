(ns oph.heratepalvelu.tep.StatusHandler
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.external.arvo :as arvo]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.common :as c]
            [clojure.string :as str]
            [environ.core :refer [env]])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.tep.StatusHandler"
  :methods [[^:static handleEmailStatus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(def ^:private new-changes? (atom false))

(defn -handleEmailStatus [this event context]
  (log-caller-details-scheduled "handleEmailStatus" event context)
  (loop [emails (ddb/query-items {:kasittelytila [:eq [:s (:viestintapalvelussa c/kasittelytilat)]]}
                                 {:index "niputusIndex"
                                  :limit 100}
                                 (:nippu-table env))]
    (doseq [email emails]
      (let [nippu (ddb/get-item {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto email)]
                                 :niputuspvm                  [:s (:niputuspvm email)]}
                                (:nippu-table env))
            status (vp/get-email-status (:viestintapalvelu-id nippu))
            tila (if (= (:numberOfSuccessfulSendings status) 1)
                   (:success c/kasittelytilat)
                   (if (= (:numberOfBouncedSendings status) 1)
                     (:bounced c/kasittelytilat)
                     (when (= (:numberOfFailedSendings status) 1)
                       (:failed c/kasittelytilat))))]
        (if tila
          (do
            (when (not @new-changes?)
              (reset! new-changes? true))
            (try
              (when-not
                (or
                  (str/includes? (:kyselylinkki nippu) ",")
                  (str/includes? (:kyselylinkki nippu) ";"))
                (arvo/patch-nippulinkki-metadata (:kyselylinkki nippu) tila))
              (ddb/update-item
                {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]
                 :niputuspvm                  [:s (:niputuspvm nippu)]}
                {:update-expr    "SET #kasittelytila = :kasittelytila"
                 :expr-attr-names {"#kasittelytila" "kasittelytila"}
                 :expr-attr-vals  {":kasittelytila" [:s tila]}}
                (:nippu-table env))
              (catch AwsServiceException e
                (log/error "Lähetystilan tallennus kantaan epäonnistui" nippu)
                (log/error e))
              (catch Exception e
                (log/error "Lähetystilan tallennus Arvoon epäonnistui" nippu)
                (log/error e))))
          (do
            (log/info "Odottaa lähetystä viestintäpalvelussa")
            (log/info email)
            (log/info status)))))
    (when (and @new-changes?
               (< 60000 (.getRemainingTimeInMillis context)))
      (reset! new-changes? false)
      (recur (ddb/query-items {:kasittelytila [:eq [:s (:viestintapalvelussa c/kasittelytilat)]]}
                              {:index "niputusIndex"
                               :limit 10}
                              (:nippu-table env))))))
