(ns oph.heratepalvelu.tep.ehoksTimedOperationsHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.tep.ehoksTimedOperationsHandler :as etoh]
            [oph.heratepalvelu.external.ehoks :as ehoks]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(use-fixtures :each tu/clear-logs-before-test)

(def results (atom {}))
(def delete-endpoint-called (atom false))
(def scan-called (atom 0))
(def update-item-called (atom 0))

(defn- mock-get-paattyneet-tyoelamajaksot [start end limit]
  (reset! results {:start start :end end})
  {:body {:data limit}})

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

(deftest test-handleTimedOperations
  (testing "Varmista, että -handleTimedOperations kutsuu ehoks-palvelua oikein"
    (with-redefs
      [clojure.tools.logging/log* tu/mock-log*
       oph.heratepalvelu.common/local-date-now (fn [] (LocalDate/of 2021 10 10))
       ehoks/get-paattyneet-tyoelamajaksot mock-get-paattyneet-tyoelamajaksot
       ehoks/delete-tyopaikkaohjaajan-yhteystiedot mock-delete-call
       ddb/scan mock-scan
       ddb/update-item mock-update-item]
      (let [event (tu/mock-handler-event :scheduledherate)
            context (tu/mock-handler-context)
            expected {:start "2021-07-01" :end "2021-10-10"}]
        (etoh/-handleTimedOperations {} event context)
        (is (= @results expected))
        (is (true? (tu/logs-contain?
                     {:level :info
                      :message "Käynnistetään jaksojen lähetys"})))
        (is (true? (tu/logs-contain?
                     {:level :info
                      :message "Lähetetty 1500 viestiä"})))

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
