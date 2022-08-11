(ns oph.heratepalvelu.util.dbChanger
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [environ.core :refer [env]]
            [clj-time.core :as t]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.koski :as koski]
            [oph.heratepalvelu.amis.AMISCommon :as ac]
            [oph.heratepalvelu.tep.tepCommon :as tc])
  (:import (software.amazon.awssdk.services.dynamodb.model ScanRequest AttributeValue)
           (java.time LocalDate)))

(gen-class
  :name "oph.heratepalvelu.util.dbChanger"
  :methods [[^:static handleDBUpdate
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]
            [^:static handleDBUpdateTep
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
  (loop [resp (scan {:filter-expression (str "attribute_not_exists(rahoitusryhma) "
                                             "AND alkupvm = :pvm")
                     :expr-attr-vals {":pvm" (.build (.s (AttributeValue/builder) "2022-08-11"))}})]
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
              {:rahoitusryhma [:s rahoitusryhma]})))
        (catch Exception e
          (log/error e))))
    (when (.hasLastEvaluatedKey resp)
      (recur (scan
               {:filter-expression (str "attribute_not_exists(rahoitusryhma) "
                                        "AND alkupvm = :pvm")
                :expr-attr-vals {":pvm" (.build (.s (AttributeValue/builder) "2022-07-11"))}
                :exclusive-start-key (.lastEvaluatedKey resp)})))))

(defn -handleDBUpdateTep [this event context]
  (loop [resp (scan {:filter-expression (str "jakso_loppupvm >= :pvm")
                     :expr-attr-vals {":pvm" (.build (.s (AttributeValue/builder) "2022-07-01"))}})]
    (doseq [item (map ddb/map-attribute-values-to-vals (.items resp))]
      (try
        (let [opiskeluoikeus (koski/get-opiskeluoikeus-catch-404
                               (:opiskeluoikeus_oid item))
              rahoitusryhma (c/get-rahoitusryhma opiskeluoikeus
                                                 (LocalDate/parse (:jakso_loppupvm item)))]
          (println (str rahoitusryhma " " (:jakso_loppupvm item))))
        (catch Exception e
          (log/error e))))
    (when (.hasLastEvaluatedKey resp)
      (recur (scan
               {:filter-expression (str "jakso_loppupvm >= :pvm")
                :expr-attr-vals {":pvm" (.build (.s (AttributeValue/builder) "2022-07-01"))}
                :exclusive-start-key (.lastEvaluatedKey resp)})))))

