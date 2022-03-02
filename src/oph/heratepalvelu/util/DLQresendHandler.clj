(ns oph.heratepalvelu.util.DLQresendHandler
  (:require [environ.core :refer [env]]
            [clojure.tools.logging :as log])
  (:import (software.amazon.awssdk.services.sqs SqsClient)
           (software.amazon.awssdk.regions Region)
           (software.amazon.awssdk.services.sqs.model SendMessageRequest
                                                      GetQueueUrlRequest)
           (software.amazon.awssdk.core.client.config
             ClientOverrideConfiguration)
           (com.amazonaws.xray.interceptors TracingInterceptor)))

(gen-class
  :name "oph.heratepalvelu.util.DLQresendHandler"
  :methods [[^:static handleDLQresend
             [com.amazonaws.services.lambda.runtime.events.SQSEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(def sqs-client (-> (SqsClient/builder)
                    (.region (Region/EU_WEST_1))
                    (.overrideConfiguration
                      (-> (ClientOverrideConfiguration/builder)
                          (.addExecutionInterceptor (TracingInterceptor.))
                          (.build)))
                    (.build)))

(defn- create-get-queue-url-request-builder
  "Abstraktio GetQueueUrlRequest/builderin ympäri, joka helpottaa testaamista."
  []
  (GetQueueUrlRequest/builder))

(defn- create-send-message-request-builder
  "Abstraktio SendMessageRequest/builderin ympäri, joka helpottaa testaamista."
  []
  (SendMessageRequest/builder))

(defn -handleDLQresend
  "Ottaa herätteitä vastaan AMISin dead letter queuesta ja lähettää ne
  uudestaan."
  [this event context]
  (let [messages (seq (.getRecords event))
        queue-url (.queueUrl
                    (.getQueueUrl
                      sqs-client
                      (-> (create-get-queue-url-request-builder)
                          (.queueName (:queue-name env))
                          (.build))))]
    (doseq [msg messages]
      (log/info (.getBody msg))
      (try
        (.sendMessage sqs-client (-> (create-send-message-request-builder)
                                     (.queueUrl queue-url)
                                     (.messageBody (.getBody msg))
                                     (.build)))
        (catch Exception e
          (log/error e))))))
