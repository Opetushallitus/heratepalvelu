(ns oph.heratepalvelu.util.dbChanger
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [environ.core :refer [env]]
            [clj-time.core :as t]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.koski :as koski]
            [oph.heratepalvelu.amis.AMISCommon :as ac]
            [oph.heratepalvelu.tep.tepCommon :as tc]
            [oph.heratepalvelu.tep.niputusHandler :as nip])
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
  (loop [resp (scan {:filter-expression (str "jakso_loppupvm >= :start "
                                             "AND jakso_loppupvm >= :end "
                                             "AND attribute_not_exists(uudelleenlaskettu_kesto)")
                     :expr-attr-vals {":start" (.build (.s (AttributeValue/builder) "2021-07-01"))
                                      ":end" (.build (.s (AttributeValue/builder) "2022-06-30"))}})]
    (doseq [item (map ddb/map-attribute-values-to-vals (.items resp))]
      (try
        (let [dbjakso (ddb/query-items {:ohjaaja_ytunnus_kj_tutkinto [:eq [:s (:ohjaaja_ytunnus_kj_tutkinto item)]]}
                                     {:index "niputusIndex"}
                                     (:table env))
              jakso (first dbjakso)
              oppijan-kaikki-jaksot (ddb/query-items
                                      {:oppija_oid
                                       [:eq [:s (:oppija_oid jakso)]]
                                       :niputuspvm [:eq [:s (:niputuspvm jakso)]]}
                                      {:index "tepDbChangerIndex"
                                       :filter-expression "#pvm >= :pvm AND attribute_exists(#tunnus)"
                                       :expr-attr-names {"#pvm"    "viimeinen_vastauspvm"
                                                         "#tunnus" "tunnus"}
                                       :expr-attr-vals {":pvm" [:s (:niputuspvm jakso)]}}
                                      (:table env))
              kesto (:kesto jakso)
              uudelleenlaskettu_kesto (:uudelleenlaskettu_kesto jakso)]
          (println item)
          (println jakso)
          (println oppijan-kaikki-jaksot)
          (println (nip/compute-kestot oppijan-kaikki-jaksot))
          (println (str "kesto" " " kesto))
          (println (str "uudelleenlaskettu_kesto" " " uudelleenlaskettu_kesto)))
        (catch Exception e
          (log/error e))))
    (when (.hasLastEvaluatedKey resp)
      (recur (scan
               {:filter-expression (str "jakso_loppupvm >= :start "
                                        "AND jakso_loppupvm >= :end "
                                        "AND attribute_not_exists(uudelleenlaskettu_kesto)")
                :expr-attr-vals {":start" (.build (.s (AttributeValue/builder) "2021-07-01"))
                                 ":end" (.build (.s (AttributeValue/builder) "2022-06-30"))}})))))

