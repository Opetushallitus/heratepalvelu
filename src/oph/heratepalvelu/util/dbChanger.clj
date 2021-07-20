(ns oph.heratepalvelu.util.dbChanger
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [environ.core :refer [env]]
            [clj-time.core :as t]
            [clojure.tools.logging :as log])
  (:import (software.amazon.awssdk.services.dynamodb.model ScanRequest)))

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
                  (.exclusiveStartKey (:exclusive-start-key options)))
                (.tableName (:table env))
                (.limit (int 1))
                (.build))
        response (.scan ddb/ddb-client req)]
    response))

(defn -handleDBUpdate [this event context]
  (loop [resp (scan
                {:filter-expression "attribute_exists(tyopaikkaohjaaja_puhelinnumero)"})]
    (doseq [item (map ddb/map-attribute-values-to-vals (.items resp))]
      (ddb/update-item
        {:hankkimistapa_id [:n (:hankkimistapa_id item)]}
        {:update-expr     "SET #value1 = :value1 REMOVE #value2"
         :expr-attr-names {"#value1" "ohjaaja_puhelinnumero"
                           "#value2" "tyopaikkaohjaaja_puhelinnumero"}
         :expr-attr-vals {":value1" [:s (:tyopaikkaohjaaja_puhelinnumero item)]}}
        (:table env)))
    (when (.hasLastEvaluatedKey resp)
      (recur (scan
               {:filter-expression "attribute_exists(tyopaikkaohjaaja_puhelinnumero)"
                :exclusive-start-key (.lastEvaluatedKey resp)})))))
