(ns oph.heratepalvelu.util.DLQresendHandler
  "Lähettää AMISin dead letter queuessa olevia herätteitä uudestaan."
  (:require [environ.core :refer [env]]
            [oph.heratepalvelu.log.caller-log :refer [log-caller-details-sqs]]
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
  :name "oph.heratepalvelu.util.DLQresendHandler"
  :methods [[^:static handleDLQresend
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

(defn -handleDLQresend
  "Ottaa herätteitä vastaan AMISin dead letter queuesta ja lähettää ne
  uudestaan."
  [_ ^SQSEvent event context]
  (log-caller-details-sqs "handleDLQresend" context)
  (let [messages (seq (.getRecords event))
        queue-url (.queueUrl
                    (.getQueueUrl
                      sqs-client
                      (-> (GetQueueUrlRequest/builder)
                          (.queueName (:queue-name env))
                          ^GetQueueUrlRequest (.build))))]
    (doseq [^SQSEvent$SQSMessage msg messages]
      (let [body (.getBody msg)]
        (log/info "käsitellään heräte" body)
        (try
          (.sendMessage sqs-client
                        (-> (SendMessageRequest/builder)
                            (.queueUrl queue-url)
                            (.messageBody body)
                            ^SendMessageRequest (.build)))
          (catch Exception e
            (log/error e "at -handleDLQresend")))))))
