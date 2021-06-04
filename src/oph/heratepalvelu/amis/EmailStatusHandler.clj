(ns oph.heratepalvelu.amis.EmailStatusHandler
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.external.arvo :as arvo]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.common :as c]
            [clojure.string :as str])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.amis.EmailStatusHandler"
  :methods [[^:static handleEmailStatus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])
(def ^:private new-changes? (atom false))

(defn -handleEmailStatus [this event context]
  (log-caller-details-scheduled "handleEmailStatus" event context)
  (loop [emails (ddb/query-items {:lahetystila [:eq [:s (:viestintapalvelussa c/kasittelytilat)]]}
                                 {:index "lahetysIndex"
                                  :limit 100})]
    (doseq [email emails]
      (let [status (vp/get-email-status (:viestintapalvelu-id email))
            tila (if (= (:numberOfSuccessfulSendings status) 1)
                   (:success c/kasittelytilat)
                   (if (= (:numberOfBouncedSendings email) 1)
                     (:bounced c/kasittelytilat)
                     (when (= (:numberOfFailedSendings status) 1)
                       (:failed c/kasittelytilat))))
            lahetyspvm (first (str/split (:sendingEnded status) #"T"))]
        (if tila
          (do
            (when (not @new-changes?)
              (reset! new-changes? true))
            (try
              (arvo/patch-kyselylinkki-metadata (:kyselylinkki email) tila)
              (c/send-lahetys-data-to-ehoks
                (:toimija_oppija email)
                (:tyyppi_kausi email)
                {:kyselylinkki (:kyselylinkki email)
                 :lahetyspvm lahetyspvm
                 :sahkoposti (:sahkoposti email)
                 :lahetystila tila})
              (ddb/update-item
                {:toimija_oppija [:s (:toimija_oppija email)]
                 :tyyppi_kausi   [:s (:tyyppi_kausi email)]}
                {:update-expr    "SET #lahetystila = :lahetystila"
                 :expr-attr-names {"#lahetystila" "lahetystila"}
                 :expr-attr-vals  {":lahetystila" [:s tila]}})
              (catch AwsServiceException e
                (log/error "Lähetystilan tallennus kantaan epäonnistui" email)
                (log/error e))
              (catch Exception e
                (log/error "Lähetystilan tallennus Arvoon epäonnistui" email)
                (log/error e))))
          (do
            (log/info "Odottaa lähetystä viestintäpalvelussa")
            (log/info email)
            (log/info status)))))
    (when (and @new-changes?
               (< 60000 (.getRemainingTimeInMillis context)))
      (reset! new-changes? false)
      (recur (ddb/query-items {:lahetystila [:eq [:s (:viestintapalvelussa c/kasittelytilat)]]}
                              {:index "lahetysIndex"
                               :limit 10})))))
