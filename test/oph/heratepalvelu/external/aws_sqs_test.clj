(ns oph.heratepalvelu.external.aws-sqs-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.external.aws-sqs :as sqs]
            [oph.heratepalvelu.test-util :as tu])
  (:import (clojure.lang ExceptionInfo)))

(definterface IMockSqsResponse
  (messageId []))

(deftype MockSqsResponse [id]
  IMockSqsResponse
  (messageId [this] id))

(def saved-sent-message (atom {}))

(definterface IMockSqsClient
  (sendMessage [message]))

(deftype MockSqsClient []
  IMockSqsClient
  (sendMessage [this message]
    (reset! saved-sent-message message)
    (MockSqsResponse.
      (when (.contains (:messageBody message) "non-error") 123))))

(definterface IMockSendMessageRequestBuilder
  (build [])
  (queueUrl [url])
  (messageBody [message-body]))

(deftype MockSendMessageRequestBuilder [contents]
  IMockSendMessageRequestBuilder
  (build [this] contents)
  (queueUrl [this url]
    (MockSendMessageRequestBuilder. (assoc contents :queueUrl url)))
  (messageBody [this body]
    (MockSendMessageRequestBuilder. (assoc contents :messageBody body))))

(defn- mock-create-send-message-request-builder []
  (MockSendMessageRequestBuilder. {}))

(deftest test-send-tep-sms-sqs-message
  (testing "send-tep-sms-sqs-message tekee oikeita kutsuja ja virhekäsittelyä"
    (with-redefs
      [clojure.tools.logging/log* tu/mock-log*
       environ.core/env {:sms-queue "sms-queue-url"}
       oph.heratepalvelu.external.aws-sqs/create-send-message-request-builder
       mock-create-send-message-request-builder
       oph.heratepalvelu.external.aws-sqs/sqs-client (MockSqsClient.)]
      (let [non-error-message {:text "non-error"}
            error-message {:text "asdf"}
            non-error-saved-results {:queueUrl "sms-queue-url"
                                     :messageBody "{\"text\":\"non-error\"}"}
            error-saved-results {:queueUrl "sms-queue-url"
                                 :messageBody "{\"text\":\"asdf\"}"}
            error-log-regex #"Failed to send message.*"]
        (is (nil? (sqs/send-tep-sms-sqs-message non-error-message)))
        (is (= non-error-saved-results @saved-sent-message))
        (is (thrown? ExceptionInfo
                     (sqs/send-tep-sms-sqs-message error-message)))
        (is (= error-saved-results @saved-sent-message))
        (is (tu/logs-contain-matching? :error error-log-regex))))))
