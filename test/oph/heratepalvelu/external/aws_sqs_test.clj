(ns oph.heratepalvelu.external.aws-sqs-test
  (:require [clojure.string :as s]
            [clojure.test :refer :all]
            [oph.heratepalvelu.external.aws-sqs :as sqs]
            [oph.heratepalvelu.test-util :as tu])
  (:import (clojure.lang ExceptionInfo)
           (software.amazon.awssdk.services.sqs SqsClient)
           (software.amazon.awssdk.services.sqs.model SendMessageRequest
                                                      SendMessageResponse)))

(def saved-sent-message (atom {}))

(def mockSqsClient
  (proxy [SqsClient] []
    (sendMessage [^SendMessageRequest send-msg-req]
      (let [message {:queueUrl (.queueUrl send-msg-req)
                     :messageBody (.messageBody send-msg-req)}
            resp-builder (SendMessageResponse/builder)
            id (when (s/includes? (:messageBody message) "non-error") "123")]
        (reset! saved-sent-message message)
        (.build (.messageId resp-builder id))))))

(deftest test-send-tep-sms-sqs-message
  (testing "send-tep-sms-sqs-message tekee oikeita kutsuja ja virhekäsittelyä"
    (with-redefs [clojure.tools.logging/log* tu/mock-log*
                  environ.core/env {:sms-queue "sms-queue-url"}
                  oph.heratepalvelu.external.aws-sqs/sqs-client mockSqsClient]
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
