(ns oph.heratepalvelu.migrateHandler
  (:require [environ.core :refer [env]])
  (:import (software.amazon.awssdk.services.dynamodb DynamoDbClient)
           (software.amazon.awssdk.services.dynamodb.model PutItemRequest
                                                           ScanRequest)
           (software.amazon.awssdk.regions Region)
           (software.amazon.awssdk.core.client.config ClientOverrideConfiguration)
           (com.amazonaws.xray.interceptors TracingInterceptor)))

(def ddb-client (-> (DynamoDbClient/builder)
                    (.region (Region/EU_WEST_1))
                    (.overrideConfiguration
                      (-> (ClientOverrideConfiguration/builder)
                          (.addExecutionInterceptor (TracingInterceptor.))
                          (.build)))
                    (.build)))

(gen-class
  :name "oph.heratepalvelu.migrateHandler"
  :methods [[^:static handleMigration
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn -handleMigration [this event context]
  (loop [scanreq (-> (ScanRequest/builder)
                    (.consistentRead true)
                    (.tableName (:herate-table env))
                    (.build))]
    (let [res (.scan ddb-client scanreq)]
      (doseq [item (.items res)]
        (.putItem ddb-client (-> (PutItemRequest/builder)
                                 (.tableName (:amis-table env))
                                 (.item item)
                                 (.build))))
      (when (.hasLastEvaluatedKey res)
        (recur (-> (.toBuilder scanreq)
                   (.exclusiveStartKey (.lastEvaluatedKey res))
                   (.build)))))))
