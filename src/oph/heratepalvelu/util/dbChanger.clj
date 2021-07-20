(ns oph.heratepalvelu.util.dbChanger
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [environ.core :refer [env]]
            [clj-time.core :as t]
            [clojure.tools.logging :as log])
  (:import (software.amazon.awssdk.services.dynamodb.model ScanRequest AttributeValue)))

(gen-class
  :name "oph.heratepalvelu.util.dbChanger"
  :methods [[^:static handleDBUpdate
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn scan [options]
  (let [req (-> (ScanRequest/builder)
                (cond->
                  (:filter-expression options)
                  (.filterExpression (:filter-expression options))
                  (:exclusive-start-key options)
                  (.exclusiveStartKey (:exclusive-start-key options))
                  (:expr-attr-vals options)
                  (.expressionAttributeValues (:expr-attr-vals options)))
                (.tableName (:table env))
                (.build))
        response (.scan ddb/ddb-client req)]
    (log/info (count (.items response)))
    response))

(defn -handleDBUpdate [this event context]
  (loop [resp (scan
                {:filter-expression "sms_kasittelytila = :value1"
                 :expr-attr-vals    {":value1" (.build (.s (AttributeValue/builder) "phone-mismatch"))}})]
    (doseq [item (map ddb/map-attribute-values-to-vals (.items resp))]
      (ddb/update-item
        {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto item)]
         :niputuspvm                  [:s (:niputuspvm item)]}
        {:update-expr     "SET #value1 = :value1"
         :expr-attr-names {"#value1" "sms_kasittelytila"}
         :expr-attr-vals {":value1" [:s "ei_lahetetty"]}}
        (:table env)))
    (when (.hasLastEvaluatedKey resp)
      (recur (scan
               {:filter-expression "sms_kasittelytila = :value1"
                :expr-attr-vals {":value1" (.build (.s (AttributeValue/builder) "phone-mismatch"))}
                :exclusive-start-key (.lastEvaluatedKey resp)})))))
