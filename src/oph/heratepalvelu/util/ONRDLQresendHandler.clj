(ns oph.heratepalvelu.util.ONRDLQresendHandler
  "Lähettää ONRhenkilomodify dead letter queuessa olevia herätteitä uudestaan."
  (:require [environ.core :refer [env]]
            [clojure.tools.logging :as log])
  (:import (software.amazon.awssdk.core.client.config
             ClientOverrideConfiguration)
           (com.amazonaws.services.lambda.runtime.events SQSEvent
                                                         SQSEvent$SQSMessage)
           (com.amazonaws.xray.interceptors TracingInterceptor)
           (software.amazon.awssdk.regions Region)
           (software.amazon.awssdk.services.sqs SqsClient)
           (software.amazon.awssdk.services.sqs.model SendMessageRequest
                                                      GetQueueUrlRequest)))

(gen-class
  :name "oph.heratepalvelu.util.ONRDLQresendHandler"
  :methods [[^:static handleONRDLQresend
             [com.amazonaws.services.lambda.runtime.events.SQSEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(def ^SqsClient sqs-client
  "SQS-client -objekti."
  (-> (SqsClient/builder)
      (.region (Region/EU_WEST_1))
      (.overrideConfiguration
        (-> (ClientOverrideConfiguration/builder)
            (.addExecutionInterceptor (TracingInterceptor.))
            ^ClientOverrideConfiguration (.build)))
      (.build)))

(defn -handleONRDLQresend
  "Ottaa herätteitä vastaan ONRhenkilomodify dead letter queuesta ja lähettää ne
  uudestaan käsiteltäväksi."
  [_ ^SQSEvent event _]
  (let [messages (seq (.getRecords event))
        queue-url (.queueUrl
                    (.getQueueUrl
                      sqs-client
                      (-> (GetQueueUrlRequest/builder)
                          (.queueName (:queue-name env))
                          ^GetQueueUrlRequest (.build))))]
    (doseq [^SQSEvent$SQSMessage msg messages]
      (log/info "käsitellään SQS-heräte:" (.getBody msg))
      (try
        (.sendMessage sqs-client
                      (-> (SendMessageRequest/builder)
                          (.queueUrl queue-url)
                          (.messageBody (.getBody msg))
                          ^SendMessageRequest (.build)))
        (catch Exception e
          (log/error e))))))
