(ns oph.heratepalvelu.util.DLQresendHandler-test
  (:require [cheshire.core :refer [generate-string]]
            [clojure.test :refer :all]
            [oph.heratepalvelu.util.DLQresendHandler :as dlqrh]
            [oph.heratepalvelu.test-util :as tu])
  (:import (software.amazon.awssdk.services.sqs SqsClient SqsClientBuilder)
           (software.amazon.awssdk.services.sqs.model SendMessageRequest
                                                      GetQueueUrlRequest
                                                      GetQueueUrlResponse)))

(def test-handleDLQresend-results (atom []))

(def mockSqsClient
  (proxy [SqsClient] []
    (getQueueUrl [^GetQueueUrlRequest queue-url-req]
      (let [builder (GetQueueUrlResponse/builder)]
        (.build (.queueUrl builder (.queueName queue-url-req)))))
    (sendMessage [^SendMessageRequest send-message-req]
      (reset! test-handleDLQresend-results
              (cons {:message-body (.messageBody send-message-req)
                     :queue-url (.queueUrl send-message-req)}
                    @test-handleDLQresend-results)))))

(deftest test-handleDLQresend
  (testing "Varmista, että -handleDLQresend toimii oikein"
    (with-redefs
      [environ.core/env {:queue-name "queue-name"}
       oph.heratepalvelu.util.DLQresendHandler/sqs-client mockSqsClient]
      (let [event (tu/mock-sqs-event {:sqs-message "asdf"})
            context (tu/mock-handler-context)
            results [{:message-body "{\"sqs-message\":\"asdf\"}"
                      :queue-url "queue-name"}]]
        (dlqrh/-handleDLQresend {} event context)
        (is (= results (vec (reverse @test-handleDLQresend-results))))))))
