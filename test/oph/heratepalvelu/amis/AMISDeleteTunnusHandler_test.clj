(ns oph.heratepalvelu.amis.AMISDeleteTunnusHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.amis.AMISDeleteTunnusHandler :as dth]
            [oph.heratepalvelu.test-util :as tu]))

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

;; Testaa -handleDeleteTunnus
(def store-full-call-results (atom ""))

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
       oph.heratepalvelu.amis.AMISCommon/get-item-by-kyselylinkki
       mock-get-item-by-kyselylinkki
       oph.heratepalvelu.amis.AMISDeleteTunnusHandler/delete-one-item
       mock-delete-one-item]
      (let [context (tu/mock-handler-context)
            event (tu/mock-sqs-event {:kyselylinkki "https://a.com/tunnus"})
            bad-event (tu/mock-sqs-event {})]
        (dth/-handleDeleteTunnus {} event context)
        (is (= @store-full-call-results
               (str "{:kyselylinkki \"https://a.com/tunnus\", "
                    ":other-fields \"other fields\"}")))
        (reset! store-full-call-results "")
        (dth/-handleDeleteTunnus {} bad-event context)
        (is (true? (tu/did-log? ":herate {}, :msg" "ERROR")))
        (is (= @store-full-call-results ""))))))
