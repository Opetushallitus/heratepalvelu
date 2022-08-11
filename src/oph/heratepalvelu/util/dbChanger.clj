(ns oph.heratepalvelu.util.dbChanger
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [environ.core :refer [env]]
            [clj-time.core :as t]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.koski :as koski]
            [oph.heratepalvelu.amis.AMISCommon :as ac])
  (:import (software.amazon.awssdk.services.dynamodb.model ScanRequest AttributeValue)
           (java.time LocalDate)))

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
                (.tableName (:herate-table env))
                (.build))
        response (.scan ddb/ddb-client req)]
    (log/info (count (.items response)))
    response))

(defn -handleDBUpdate [this event context]
  (loop [resp (scan {:filter-expression (str "attribute_not_exists(rahoitusryhma) "
                                             "AND lahetystila = :tila")
                     :expr-attr-vals    {":tila" (.build (.s
                                                           (AttributeValue/builder)
                                                           "ei_lahetetty_TEMP_RAHOITUSRYHMA_FIX"))}})]
    (doseq [item (map ddb/map-attribute-values-to-vals (.items resp))]
      (try
        (let [opiskeluoikeus (koski/get-opiskeluoikeus-catch-404
                               (:opiskeluoikeus-oid item))
              rahoitusryhma (c/get-rahoitusryhma opiskeluoikeus
                                                 (LocalDate/parse (:alkupvm item)))]
          (println rahoitusryhma)
          (when (some? rahoitusryhma)
            (ac/update-herate
              item
              {:rahoitusryhma [:s rahoitusryhma]
               :lahetystila [:s (:ei-lahetetty c/kasittelytilat)]})))
        (catch Exception e
          (log/error e))))
    (when (.hasLastEvaluatedKey resp)
      (recur (scan
               {:filter-expression (str "attribute_not_exists(rahoitusryhma) "
                                        "AND lahetystila = :tila")
                :expr-attr-vals    {":tila" (.build (.s
                                                      (AttributeValue/builder)
                                                      "ei_lahetetty_TEMP_RAHOITUSRYHMA_FIX"))}
                :exclusive-start-key (.lastEvaluatedKey resp)})))))