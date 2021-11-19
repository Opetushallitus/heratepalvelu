(ns oph.heratepalvelu.util.patchArvoHandler
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo])
  (:import (software.amazon.awssdk.services.dynamodb.model ScanRequest
                                                           AttributeValue)))

(gen-class
  :name "oph.heratepavlelu.util.patchArvoHandler"
  :methods [[^:static handlePatchArvo
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

(def filter-expression
  "attribute_not_exists(arvo_patchattu) AND jakso_loppupvm >= :pvm")

(def expr-attr-vals
  {":pvm" (.build (.s (AttributeValue/builder) "2021-07-01"))})

(defn -handlePatchArvo [this event context]
  (loop [resp (scan {:filter-expression filter-expression
                     :expr-attr-vals expr-attr-vals})]
    (doseq [item (map ddb/map-attribute-values-to-vals (.items resp))]
      (try
        (arvo/patch-vastaajatunnus
          (:tunnus item)
          (if (:oppisopimuksen_perusta item)
            {:oppisopimuksen_perusta (:oppisopimuksen_perusta item)
             :tyopaikka_normalisoitu (:tyopaikan_normalisoitu_nimi item)}
            {:tyopaikka_normalisoitu (:tyopaikan_normalisoitu_nimi item)}))
        (ddb/update-item
          {:hankkimistapa_id [:n (:hankkimistapa_id item)]}
          {:update-expr "SET arvo_patchattu = :value"
           :expr-attr-vals {":value" [:bool true]}}
          (:table env))
        (catch Exception e
          (log/error e))))
    (when (.hasLastEvaluatedKey resp)
      (recur (scan {:exclusive-start-key (.lastEvaluatedKey resp)
                    :filter-expression filter-expression
                    :expr-attr-vals expr-attr-vals})))))
