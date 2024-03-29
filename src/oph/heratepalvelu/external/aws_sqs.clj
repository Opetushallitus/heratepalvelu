(ns oph.heratepalvelu.external.aws-sqs
  "Wrapperit SQS:n ympäri."
  (:require [environ.core :refer [env]]
            [clojure.data.json :as json]
            [clojure.tools.logging :as log])
  (:import (software.amazon.awssdk.services.sqs SqsClient)
           (software.amazon.awssdk.regions Region)
           (software.amazon.awssdk.services.sqs.model SendMessageRequest)))

(def ^:private ^SqsClient sqs-client
  "SQS-client -objekti."
  (-> (SqsClient/builder)
      (.region (Region/EU_WEST_1))
      (.build)))

(defn send-tep-sms-sqs-message
  "Lähettää SQS-viestin SMS-queueen."
  [msg]
  (let [resp (.sendMessage sqs-client (-> (SendMessageRequest/builder)
                                          (.queueUrl (:sms-queue env))
                                          (.messageBody (json/write-str msg))
                                          ^SendMessageRequest (.build)))]
    (when-not (some? (.messageId resp))
      (log/error "Failed to send message " msg)
      (throw (ex-info "Failed to send SQS message" {:error :sqs-error})))))
