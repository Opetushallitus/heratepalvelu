(ns oph.heratepalvelu.util.DLQresendHandler
  "Lähettää AMISin dead letter queuessa olevia herätteitä uudestaan."
  (:require [environ.core :refer [env]]
            [clojure.tools.logging :as log])
  (:import (software.amazon.awssdk.core.client.config
             ClientOverrideConfiguration
             ClientOverrideConfiguration$Builder)
           (com.amazonaws.services.lambda.runtime.events SQSEvent$SQSMessage)
           (com.amazonaws.xray.interceptors TracingInterceptor)
           (software.amazon.awssdk.services.sqs SqsClient SqsClientBuilder)
           (software.amazon.awssdk.regions Region)
           (software.amazon.awssdk.services.sqs.model
             SendMessageRequest
             SendMessageRequest$Builder
             GetQueueUrlRequest
             GetQueueUrlRequest$Builder)))

(gen-class
  :name "oph.heratepalvelu.util.DLQresendHandler"
  :methods [[^:static handleDLQresend
             [com.amazonaws.services.lambda.runtime.events.SQSEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(def ^SqsClient sqs-client
  "SQS-client -objekti."
  (-> ^SqsClientBuilder (SqsClient/builder)
      (.region (Region/EU_WEST_1))
      (.overrideConfiguration
        (-> ^ClientOverrideConfiguration$Builder
            (ClientOverrideConfiguration/builder)
            (.addExecutionInterceptor (TracingInterceptor.))
            ^ClientOverrideConfiguration (.build)))
      (.build)))

(defn- create-get-queue-url-req-builder
  "Abstraktio GetQueueUrlRequest/builderin ympäri, joka helpottaa testaamista."
  []
  (GetQueueUrlRequest/builder))

(defn- create-send-message-req-builder
  "Abstraktio SendMessageRequest/builderin ympäri, joka helpottaa testaamista."
  []
  (SendMessageRequest/builder))

(defn -handleDLQresend
  "Ottaa herätteitä vastaan AMISin dead letter queuesta ja lähettää ne
  uudestaan."
  [this ^com.amazonaws.services.lambda.runtime.events.SQSEvent event context]
  (let [messages (seq (.getRecords event))
        queue-url (.queueUrl
                    (.getQueueUrl
                      sqs-client
                      (-> ^GetQueueUrlRequest$Builder
                          (create-get-queue-url-req-builder)
                          (.queueName (:queue-name env))
                          ^GetQueueUrlRequest (.build))))]
    (doseq [^SQSEvent$SQSMessage msg messages]
      (log/info (.getBody msg))
      (try
        (.sendMessage sqs-client
                      (-> ^SendMessageRequest$Builder
                          (create-send-message-req-builder)
                          (.queueUrl queue-url)
                          (.messageBody (.getBody msg))
                          ^SendMessageRequest (.build)))
        (catch Exception e
          (log/error e))))))
