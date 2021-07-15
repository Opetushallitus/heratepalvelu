(ns oph.heratepalvelu.external.aws-sqs
  (:require [environ.core :refer [env]]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log])
  (:import (software.amazon.awssdk.services.sqs SqsClient)
           (software.amazon.awssdk.regions Region)
           (software.amazon.awssdk.services.sqs.model
             SendMessageRequest
             GetQueueUrlRequest
             QueueDoesNotExistException)))

(def ^:private sqs-client
    (-> (SqsClient/builder)
        (.region (Region/EU_WEST_1))
        (.build)))

(defn- get-queue-url [queue-name]
  (when (some? sqs-client)
    (if (nil? (:stage env))
      (log/warn "Stage missing from env variables")
      (.queueUrl (.getQueueUrl
                   sqs-client
                   (-> (GetQueueUrlRequest/builder)
                       (.queueName
                         (str (:stage env) "-"
                              queue-name))
                       (.build)))))))

(defn- get-queue-url-with-error-handling [queue-name]
  (try
    (get-queue-url queue-name)
    (catch QueueDoesNotExistException e
      (log/error (str queue-name " does not exist")))))

(def ^:private sms-queue-url
  (delay
    (get-queue-url-with-error-handling
      "services-heratepalvelu-TepSmsQueue")))

(defn build-sms-sqs-message [kyselylinkki oppilaitokset phonenumber
                             ohjaaja_ytunnus_kj_tutkinto niputuspvm & muistutus]
  {:linkki kyselylinkki
   :oppilaitokset oppilaitokset
   :phonenumber phonenumber
   :ohjaaja_ytunnus_kj_tutkinto ohjaaja_ytunnus_kj_tutkinto
   :niputuspvm niputuspvm
   :muistutus muistutus})

(defn send-tep-sms-sqs-message [msg]
  (let [resp (.sendMessage sqs-client (-> (SendMessageRequest/builder)
                                          (.queueUrl "queue-url")
                                          (.messageBody (json/write-str msg))
                                          (.build)))]
    (when-not (some? (.messageId resp))
      (log/error "Failed to send message " msg)
      (throw (ex-info
               "Failed to send SQS message"
               {:error :sqs-error})))))
