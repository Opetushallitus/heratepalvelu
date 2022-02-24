(ns oph.heratepalvelu.tep.tpkArvoCallHandler
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.log.caller-log :refer :all])
  (:import (clojure.lang ExceptionInfo)))

;; TODO class



;; TODO do-scan (with and without last-evaluated-key

;; TODO make-arvo-request (takes nippu and request-id)

(defn -handleTpkArvoCalls [this event context]
  (log-caller-details-scheduled "handleTpkArvoCalls" event context)
  (loop [scan-results (do-scan)]
    (doseq [nippu (:items scan-results)]
      (when (< 30000 (.getRemainingTimeInMillis context))
        (let [request-id (c/generate-uuid)
              arvo-resp (make-arvo-request nippu request-id)]
          (if (some? (:kysely_linkki arvo-resp))
            (try
              (ddb/update-item
                {:nippu-id            [:s (:nippu-id nippu)]
                 :tiedonkeruu-alkupvm [:s (:tiedonkeruu-alkupvm nippu)]}
                {:update-expr (str "SET #linkki = :linkki, #tunnus = :tunnus, "
                                   "#pvm = :pvm, #req = :req")
                 :expr-attr-names {"#linkki" "kyselylinkki"
                                   "#tunnus" "tunnus"
                                   "#pvm"    "voimassa-loppupvm"
                                   "#req"    "request-id"}
                 :expr-attr-vals {":linkki" [:s (:kysely_linkki arvo-resp)]
                                  ":tunnus" [:s (:tunnus arvo-resp)]
                                  ":pvm"    [:s (:voimassa_loppupvm arvo-resp)]
                                  ":req"    [:s request-id]}}
                (:tpk-nippu-table env))
              (catch AwsServiceException e
                (log/error "Virhe DynamoDBissa (TPK Arvo Calls). Nippu:"
                           (:nippu-id nippu)
                           "Virhe:"
                           e)))
            (log/error "Kyselylinkkiä ei saatu Arvolta. Nippu:"
                       (:nippu-id nippu))))))
    (when (and (< 30000 (.getRemainingTimeInMillis context))
               (:last-evaluated-key scan-results))
      (recur (do-scan (:last-evaluated-key scan-results))))))
