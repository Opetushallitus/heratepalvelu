(ns oph.heratepalvelu.util.rahoitusryhmaJalkiajo
  (:require [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [environ.core :refer [env]]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.external.arvo :as arvo])
  (:import (software.amazon.awssdk.services.dynamodb.model ScanRequest AttributeValue)))

(gen-class
  :name "oph.heratepalvelu.util.rahoitusryhmaJalkiajo"
  :methods [[^:static handleRahoitusRyhmaJalkiajo
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
                (.tableName (:jaksotunnus-table env))
                (.build))
        response (.scan ddb/ddb-client req)]
    (log/info (count (.items response)))
    response))

(defn -handleRahoitusRyhmaJalkiajo [this event context]
  (loop [resp (scan {:filter-expression (str "attribute_not_exists(rahoitusryhma) "
                                             "AND loppupvm >= :pvm ")
                     :expr-attr-vals {":pvm" (.build (.s (AttributeValue/builder) "2022-07-01"))}})]
    (doseq [item (map ddb/map-attribute-values-to-vals (.items resp))]
      (try
        (let [rahoitusryhma (c/get item)]
          (println rahoitusryhma)
          (if rahoitusryhma
            (ddb/update-item
              {:hankkimistapa_id [:n (:hankkimistapa_id item)]}
              {:update-expr "SET #value1 = :value1"
               :expr-attr-names {"#value1" "rahoitusryhma"}
               :expr-attr-vals {":value1" [:s rahoitusryhma]}}
              (:jaksotunnus-table env))
            (arvo/patch-vastaajatunnus (:tunnus item) {:rahoitusryhma rahoitusryhma})
        (catch Exception e
          (log/error e))))
    (when (.hasLastEvaluatedKey resp)
      (recur (scan {:exclusive-start-key (.lastEvaluatedKey resp)
                    :filter-expression (str "attribute_not_exists(oppisopimuksen_perusta) "
                                            "AND hankkimistapa_tyyppi = :htp "
                                            "AND jakso_loppupvm >= :pvm "
                                            "AND dbchangerin_kasittelema <> :dbc")
                    :expr-attr-vals {":pvm" (.build (.s (AttributeValue/builder) "2022-07-01"))}})))))))