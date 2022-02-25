(ns oph.heratepalvelu.tep.StatusHandler
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.tep.tepCommon :as tc])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.tep.StatusHandler"
  :methods [[^:static handleEmailStatus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(def ^:private new-changes? (atom false))

(defn -handleEmailStatus
  "Hakee nippuja, joilla on sähköpostiviestejä viestintäpalvelussa, ja päivittää
  niiden tiedot tietokantaan ja Arvoon. Laskee uuden loppupäivämäärän nipulle,
  jos kyselyyn ei ole vielä vastattu ja kyselyä ei ole vielä lähetetty."
  [this event context]
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
            tila (vp/convert-email-status status)
            new-loppupvm (tc/get-new-loppupvm nippu)]
        (if tila
          (do
            (when (not @new-changes?)
              (reset! new-changes? true))
            (try
              (when-not
                (or (str/includes? (:kyselylinkki nippu) ",")
                    (str/includes? (:kyselylinkki nippu) ";"))
                (arvo/patch-nippulinkki
                  (:kyselylinkki nippu)
                  (if (and new-loppupvm (= tila (:success c/kasittelytilat)))
                    {:tila tila :voimassa_loppupvm new-loppupvm}
                    {:tila tila})))
              (tc/update-nippu
                nippu
                {:kasittelytila [:s tila]
                 :voimassaloppupvm [:s (or new-loppupvm
                                           (:voimassaloppupvm nippu))]})
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
