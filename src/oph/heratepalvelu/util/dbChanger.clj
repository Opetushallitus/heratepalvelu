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
  (:import (software.amazon.awssdk.services.dynamodb.model
             ScanRequest AttributeValue)
           (java.time LocalDate)))

(gen-class
  :name "oph.heratepalvelu.util.dbChanger"
  :methods [[^:static handleDBUpdate
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]
            [^:static handleDBUpdateTep
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]
            [^:static deleteEmptyRowsFromJaksotunnusTable
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

(defn- attr [^String input] (.build (.s (AttributeValue/builder) input)))

(defn -handleDBUpdate [this event context]
  (loop [resp (scan {:filter-expression
                     "attribute_not_exists(rahoitusryhma) AND alkupvm = :pvm"
                     :expr-attr-vals {":pvm" (attr "2022-08-11")}})]
    (doseq [item (map ddb/map-attribute-values-to-vals (.items resp))]
      (try
        (let [opiskeluoikeus (koski/get-opiskeluoikeus-catch-404
                               (:opiskeluoikeus-oid item))
              rahoitusryhma (c/get-rahoitusryhma
                              opiskeluoikeus (LocalDate/parse (:alkupvm item)))]
          (when (some? rahoitusryhma)
            (ac/update-herate
              item
              {:rahoitusryhma [:s rahoitusryhma]})))
        (catch Exception e
          (log/error e))))
    (when (.hasLastEvaluatedKey resp)
      (recur (scan
               {:filter-expression
                "attribute_not_exists(rahoitusryhma) AND alkupvm = :pvm"
                :expr-attr-vals {":pvm" (attr "2022-07-11")}
                :exclusive-start-key (.lastEvaluatedKey resp)})))))

(defn -handleDBUpdateTep [this event context]
  (loop [resp (scan {:filter-expression
                     (str "jakso_loppupvm >= :start "
                          "AND jakso_loppupvm <= :end "
                          "AND attribute_not_exists(uudelleenlaskettu_kesto)")
                     :expr-attr-vals {":start" (attr "2021-11-01")
                                      ":end" (attr "2021-12-31")}})]
    (doseq [item (map ddb/map-attribute-values-to-vals (.items resp))]
      (try
        (let [dbjakso (ddb/query-items
                        {:ohjaaja_ytunnus_kj_tutkinto
                         [:eq [:s (:ohjaaja_ytunnus_kj_tutkinto item)]]}
                        {:index "niputusIndex"}
                        (:table env))
              jakso (first dbjakso)
              uudelleenlaskettu_kesto (:uudelleenlaskettu_kesto jakso)]
          (when (nil? uudelleenlaskettu_kesto)
            (println (str "Lasketaan oppijan "
                          (:oppija_oid jakso)
                          " jaksojen kestot uudelleen niputuksessa "
                          (:niputuspvm jakso)))
            (let [oppijan-kaikki-jaksot
                  (ddb/query-items
                    {:oppija_oid [:eq [:s (:oppija_oid jakso)]]}
                    {:index "tepDbChangerIndex"
                     :filter-expression (str "jakso_loppupvm >= :start "
                                             "jakso_loppupvm <= :end"
                                             "AND attribute_exists(#tunnus)")
                     :expr-attr-names {"#tunnus" "tunnus"}
                     :expr-attr-vals {":start" (attr "2021-11-01")
                                      ":end" (attr "2021-12-31")}}
                    (:table env))
                  uudelleenlasketut-kestot
                  (nip/compute-kestot oppijan-kaikki-jaksot)]
              (println uudelleenlasketut-kestot)
              (doseq [jakso oppijan-kaikki-jaksot]
                (println "Päivitetään jaksolle"
                         (:hankkimistapa_id jakso)
                         "uudelleenlaskettu_kesto.")
                (println "Vanha kesto"
                         (:kesto jakso)
                         "- Uudelleen laskettu kesto"
                         (nip/math-round (get uudelleenlasketut-kestot
                                              (:hankkimistapa_id jakso)
                                              0.0)))))))
        (catch Exception e (log/error e))))
    (when (.hasLastEvaluatedKey resp)
      (recur (scan
               {:filter-expression
                (str "jakso_loppupvm >= :start "
                     "AND jakso_loppupvm <= :end "
                     "AND attribute_not_exists(uudelleenlaskettu_kesto)")
                :expr-attr-vals {":start" (attr "2021-11-01")
                                 ":end" (attr "2021-12-31")}})))))
