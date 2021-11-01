(ns oph.heratepalvelu.util.dbArchiver
  (:require 
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.db.dynamodb :as ddb])
  (:import (software.amazon.awssdk.services.dynamodb.model AttributeValue
                                                           ScanRequest)))

(gen-class
  :name "oph.heratepalvelu.util.dbArchiver"
  :methods [[^:static handleDBArchiving
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
                (.tableName (:from-table env))
                (.build))
        response (.scan ddb/ddb-client req)]
    (log/info (count (.items response)))
    response))

(def kausi "2019-2020")

(defn -handleDBArchiving [this event context]
  (loop [resp (scan {:filter-expression "rahoituskausi = :kausi"
                     :expr-attr-vals    {":kausi" kausi}})]
    (doseq [item (map ddb/map-attribute-values-to-vals (.items resp))]
      (try
        (ddb/put-item item {} (:to-table env))
        (ddb/delete-item {:toimija_oppija [:s (:toimija_oppija item)]
                          :tyyppi_kausi   [:s (:tyyppi_kausi item)]})
        (catch Exception e
          (log/error "Linkin arkistointi epäonnistui:"
                     (:kyselylinkki item)
                     (ex-info e)))))
    (when (.hasLastEvaluatedKey resp)
      (recur (scan {:filter-expression   "rahoituskausi = :kausi"
                    :expr-attr-vals      {":kausi" kausi}
                    :exclusive-start-key (.lastEvaluatedKey resp)})))))
