(ns oph.heratepalvelu.tep.contactInfoCleaningHandler-test
  (:require [clojure.test :refer [deftest is testing]]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.ehoks :as ehoks]
            [oph.heratepalvelu.tep.contactInfoCleaningHandler
             :refer [-cleanContactInfo]]
            [oph.heratepalvelu.test-util :as tu]))

(def delete-endpoint-called (atom false))
(def scan-called (atom 0))
(def update-item-called (atom 0))

(defn- mock-delete-call []
  (reset! delete-endpoint-called true)
  {:body {:data {:hankkimistapa-ids [1 2 3]}}})

(defn- mock-scan [_ __]
  (swap! scan-called inc)
  {:items [{:hankkimistapa_id [:n @scan-called]
            :sahkoposti [:s "testi@oph.fi"]
            :puhelinnumero [:s "0401111111"]}]})

(defn- mock-update-item [_ __ ___]
  (swap! update-item-called inc))

(deftest test-cleanContactInfo
  (testing "Varmista, että -cleanContactInfo siivoaa yhteystiedot"
    (with-redefs
      [log/log* tu/mock-log*
       ehoks/delete-tyopaikkaohjaajan-yhteystiedot mock-delete-call
       ddb/scan mock-scan
       ddb/update-item mock-update-item]
      (let [event (tu/mock-handler-event :scheduledherate)
            context (tu/mock-handler-context)]
        (-cleanContactInfo {} event context)
        (is (true?
              (tu/logs-contain?
                {:level :info
                 :message
                 "Käynnistetään työpaikkaohjaajan yhteystietojen poisto"})))
        (is (= @scan-called 3))
        (is (= @update-item-called 3))
        (is (true? @delete-endpoint-called))
        (is (true? (tu/logs-contain?
                     {:level :info
                      :message "Poistettu 3 ohjaajan yhteystiedot"})))))))
