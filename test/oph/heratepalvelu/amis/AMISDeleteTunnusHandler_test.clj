(ns oph.heratepalvelu.amis.AMISDeleteTunnusHandler-test
  (:require [cheshire.core :refer [generate-string]]
            [clojure.test :refer :all]
            [oph.heratepalvelu.amis.AMISDeleteTunnusHandler :as dth]
            [oph.heratepalvelu.test-util :as tu])
  (:import (com.amazonaws.services.lambda.runtime.events SQSEvent
                                                         SQSEvent$SQSMessage)))

(deftest test-schema-validation
  (testing "Varmista, että scheman validointi toimii oikein"
    (let [good {:kyselylinkki "https://a.b.com/tunnus"}
          bad1 {:ei-kyselylinkki 1324}
          bad2 {:kyselylinkki ""}]
      (is (nil? (dth/delete-tunnus-checker good)))
      (is (some? (dth/delete-tunnus-checker bad1)))
      (is (some? (dth/delete-tunnus-checker bad2))))))

(defn- mock-delete-item [params]
  (and (:toimija_oppija params)
       (:tyyppi_kausi params)
       (= :s (first (:toimija_oppija params)))
       (= :s (first (:tyyppi_kausi params)))
       (= "toimija-oppija" (second (:toimija_oppija params)))
       (= "tyyppi-kausi" (second (:tyyppi_kausi params)))))

(deftest test-delete-one-item
  (testing "delete-one-item kutsuu delete-item oikeilla parametreilla"
    (with-redefs [oph.heratepalvelu.db.dynamodb/delete-item mock-delete-item]
      (is (dth/delete-one-item {:toimija_oppija "toimija-oppija"
                                :tyyppi_kausi   "tyyppi-kausi"})))))


(defn- mock-query-items [query-params index-data]
  (when (and (:kyselylinkki query-params)
             (= :eq (first (:kyselylinkki query-params)))
             (= :s (first (second (:kyselylinkki query-params))))
             (= "mock-kyselylinkki"
                (second (second (:kyselylinkki query-params))))
             (= "resendIndex" (:index index-data)))
    [true]))

(deftest test-get-item-by-kyselylinkki
  (testing "get-item-by-kyselylinkki kutsuu query-items oikeilla parametreilla"
    (with-redefs [oph.heratepalvelu.db.dynamodb/query-items mock-query-items]
      (is (dth/get-item-by-kyselylinkki "mock-kyselylinkki")))))


(def store-full-call-results (atom ""))

(defn- mock-sqs-event [item]
  (let [event (SQSEvent.)
        message (SQSEvent$SQSMessage.)]
    (.setBody message (generate-string item))
    (.setRecords event [message])
    event))

(defn- mock-delete-one-item [item]
  (reset! store-full-call-results (str @store-full-call-results item)))

(defn- mock-get-item-by-kyselylinkki [kyselylinkki]
  {:kyselylinkki kyselylinkki
   :other-fields "other fields"})

(defn- mock-error-logger [s & args]
  (reset! store-full-call-results (str @store-full-call-results s (str args))))

(deftest test-full-call-handleDeleteTunnus
  (testing "Call -handleDeleteTunnus with mocked DB and event"
    (with-redefs
      [clojure.tools.logging/error mock-error-logger
       oph.heratepalvelu.amis.AMISDeleteTunnusHandler/delete-one-item
       mock-delete-one-item
       oph.heratepalvelu.amis.AMISDeleteTunnusHandler/get-item-by-kyselylinkki
       mock-get-item-by-kyselylinkki]
      (let [context (tu/mock-handler-context)
            event (mock-sqs-event {:kyselylinkki "https://a.com/tunnus"})
            bad-event (mock-sqs-event {})]
        (dth/-handleDeleteTunnus {} event context)
        (is (= @store-full-call-results
               (str "{:kyselylinkki \"https://a.com/tunnus\", "
                    ":other-fields \"other fields\"}")))
        (reset! store-full-call-results "")
        (dth/-handleDeleteTunnus {} bad-event context)
        (is (true? (tu/did-log? ":herate {}, :msg" "ERROR")))
        (is (= @store-full-call-results ""))))))
