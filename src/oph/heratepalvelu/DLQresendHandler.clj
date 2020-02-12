(ns oph.heratepalvelu.DLQresendHandler
  (:require [environ.core :refer [env]]
            [clojure.tools.logging :as log])
  (:import (software.amazon.awssdk.services.sqs SqsClient)
           (software.amazon.awssdk.regions Region)
           (software.amazon.awssdk.services.sqs.model SendMessageRequest
                                                      GetQueueUrlRequest)
           (software.amazon.awssdk.core.client.config ClientOverrideConfiguration)
           (com.amazonaws.xray.interceptors TracingInterceptor)))

(gen-class
  :name "oph.heratepalvelu.DLQresendHandler"
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

(defn -handleDLQresend [this event context]
  (let [messages (seq (.getRecords event))
        queue-url (.queueUrl
                    (.getQueueUrl
                      sqs-client
                      (-> (GetQueueUrlRequest/builder)
                          (.queueName (:queue-name env))
                          (.build))))]
    (doseq [msg messages]
      (log/info (.getBody msg))
      (try
        (.sendMessage sqs-client (-> (SendMessageRequest/builder)
                                     (.queueUrl queue-url)
                                     (.messageBody (.getBody msg))
                                     (.build)))
        (catch Exception e
          (log/error e))))))
