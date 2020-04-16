(ns oph.heratepalvelu.EmailStatusHandler
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.log.caller-log :refer :all])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.EmailStatusHandler"
  :methods [[^:static handleEmailStatus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn -handleEmailStatus [this event context]
  (log-caller-details "handleEmailStatus" event context)
  (loop [emails (ddb/query-items {:lahetystila [:eq [:s "viestintapalvelussa"]]}
                                       {:index "lahetysIndex"
                                        :limit 100})]
    (doseq [email emails]
      (let [status (vp/get-email-status (:viestintapalvelu-id email))]
        (try
          (if (= (:numberOfSuccessfulSendings status) 1)
            (ddb/update-item
              {:toimija_oppija [:s (:toimija_oppija email)]
               :tyyppi_kausi   [:s (:tyyppi_kausi email)]}
              {:update-expr    "#lahetystila = :lahetystila"
               :expr-attr-names {"#lahetystila" "lahetystila"}
               :expr-attr-vals  {":lahetystila" [:s "lahetys_onnistunut"]}})
            (when (or (not= (:numberOfFailedSendings email) 0)
                      (not= (:numberOfBouncedSendings email) 0))
              (log/warn status)
              (ddb/update-item
                {:toimija_oppija [:s (:toimija_oppija email)]
                 :tyyppi_kausi   [:s (:tyyppi_kausi email)]}
                {:update-expr    "#lahetystila = :lahetystila"
                 :expr-attr-names {"#lahetystila" "lahetystila"}
                 :expr-attr-vals  {":lahetystila" [:s "lahetys_epaonnistunut"]}})))
          (catch AwsServiceException e
            (log/error "Tarkistustilan tallennus epäonnistui" email)
            (log/error e))
          (catch Exception e
            (log/error email)
            (log/error e)))))
    (when (and (seq emails)
               (< 30000 (.getRemainingTimeInMillis context)))
      (recur (ddb/query-items {:lahetystila [:eq [:s "viestintapalvelussa"]]}
                              {:index "lahetysIndex"
                               :limit 100})))))
