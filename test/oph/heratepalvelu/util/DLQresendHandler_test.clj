(ns oph.heratepalvelu.util.DLQresendHandler-test
  (:require [cheshire.core :refer [generate-string]]
            [clojure.test :refer :all]
            [oph.heratepalvelu.util.DLQresendHandler :as dlqrh]
            [oph.heratepalvelu.test-util :as tu]))

(def test-handleDLQresend-results (atom []))

(definterface IMockQueueUrl (queueUrl []))

(deftype MockQueueUrl [contents]
  IMockQueueUrl
  (queueUrl [this] (generate-string contents)))

(definterface IMockSqsClient
  (getQueueUrl [queue-url-builder])
  (sendMessage [send-message-request]))

(deftype MockSqsClient []
  IMockSqsClient
  (getQueueUrl [this queue-url-builder]
    (MockQueueUrl. {:queue-url-builder queue-url-builder}))
  (sendMessage [this send-message-request]
    (reset! test-handleDLQresend-results
            (cons {:message-request send-message-request}
                  @test-handleDLQresend-results))))

(definterface IMockGetQueueUrlRequest
  (build [])
  (queueName [queue-name]))

(deftype MockGetQueueUrlRequest [contents]
  IMockGetQueueUrlRequest
  (build [this] contents)
  (queueName [this queue-name]
    (MockGetQueueUrlRequest. (assoc contents :name queue-name))))

(definterface IMockSendMessageRequest
  (build [])
  (messageBody [message-body])
  (queueUrl [queue-url]))

(deftype MockSendMessageRequest [contents]
  IMockSendMessageRequest
  (build [this] contents)
  (messageBody [this message-body]
    (MockSendMessageRequest. (assoc contents :message-body message-body)))
  (queueUrl [this queue-url]
    (MockSendMessageRequest. (assoc contents :queue-url queue-url))))

(defn- mock-create-get-queue-url-req-builder []
  (MockGetQueueUrlRequest. {}))

(defn- mock-create-send-message-req-builder [] (MockSendMessageRequest. {}))

(deftest test-handleDLQresend
  (testing "Varmista, että -handleDLQresend toimii oikein"
    (with-redefs
      [environ.core/env {:queue-name "queue-name"}
       oph.heratepalvelu.util.DLQresendHandler/create-get-queue-url-req-builder
       mock-create-get-queue-url-req-builder
       oph.heratepalvelu.util.DLQresendHandler/create-send-message-req-builder
       mock-create-send-message-req-builder
       oph.heratepalvelu.util.DLQresendHandler/sqs-client (MockSqsClient.)]
      (let [event (tu/mock-sqs-event {:sqs-message "asdf"})
            context (tu/mock-handler-context)
            results [{:message-request
                      {:message-body "{\"sqs-message\":\"asdf\"}"
                       :queue-url
                       "{\"queue-url-builder\":{\"name\":\"queue-name\"}}"}}]]
        (dlqrh/-handleDLQresend {} event context)
        (is (= results (vec (reverse @test-handleDLQresend-results))))))))
