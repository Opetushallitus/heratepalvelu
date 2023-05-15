(ns oph.heratepalvelu.amis.AMISehoksTimedOperationsHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.amis.AMISehoksTimedOperationsHandler :as etoh]
            [oph.heratepalvelu.external.ehoks :as ehoks]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(use-fixtures :each tu/clear-logs-before-test)

(def results (atom {}))
(def delete-endpoint-called (atom false))
(def scan-called (atom 0))
(def update-item-called (atom 0))

(defn- mock-get-retry-kyselylinkit [start end limit]
  (reset! results {:start start :end end})
  {:body {:data limit}})

(defn- mock-delete-call []
  (reset! delete-endpoint-called true)
  {:body {:data {:hoks-ids [1 2 3]}}})

(defn- mock-scan [_ __]
  (swap! scan-called inc)
  {:items [{:toimija_oppija [:s "1"]
            :tyyppi_kausi [:s "aloittaneet/2022-2023"]}]})

(defn- mock-update-item [_ __ ___]
  (swap! update-item-called inc))

(deftest test-handleAMISTimedOperations
  (testing "Varmista, että -handleAMISTimedOperations toimii oikein"
    (with-redefs
      [clojure.tools.logging/log* tu/mock-log*
       oph.heratepalvelu.common/local-date-now
       (fn [] (LocalDate/of 2022 2 2))
       oph.heratepalvelu.external.ehoks/send-kasittelemattomat-heratteet!
       mock-get-retry-kyselylinkit
       ehoks/delete-opiskelijan-yhteystiedot mock-delete-call
       ddb/scan mock-scan
       ddb/update-item mock-update-item]
      (let [event (tu/mock-handler-event :scheduledherate)
            context (tu/mock-handler-context)
            expected {:start "2021-07-01" :end "2022-02-02"}]
        (etoh/-handleAMISTimedOperations {} event context)
        (is (= @results expected))
        (is (true? (tu/logs-contain?
                     {:level :info
                      :message "Lähetetty 1000 viestiä"})))

        (is (true?
              (tu/logs-contain?
                {:level :info
                 :message
                 "Käynnistetään opiskelijan yhteystietojen poisto"})))
        (is (true? @delete-endpoint-called))
        (is (= @scan-called 3))
        (is (= @update-item-called 3))))))

(def mass-resend-results (atom []))

(defn- mock-resend-aloitusheratteet [start end]
  (reset! mass-resend-results
          (cons {:type "mock-resend-aloitusheratteet" :start start :end end}
                @mass-resend-results))
  {:body {:data {:count 123}}})

(defn- mock-resend-paattoheratteet [start end]
  (reset! mass-resend-results
          (cons {:type "mock-resend-paattoheratteet" :start start :end end}
                @mass-resend-results))
  {:body {:data {:count 456}}})

(deftest test-handleMassHerateResend
  (testing "Varmista, että -handleMassHerateResend toimii oikein"
    (with-redefs [clojure.tools.logging/log* tu/mock-log*
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 2 2))
                  oph.heratepalvelu.external.ehoks/resend-aloitusheratteet
                  mock-resend-aloitusheratteet
                  oph.heratepalvelu.external.ehoks/resend-paattoheratteet
                  mock-resend-paattoheratteet]
      (etoh/-handleMassHerateResend {}
                                    (tu/mock-handler-event :scheduledherate)
                                    (tu/mock-handler-context))
      (is (= (vec (reverse @mass-resend-results))
             [{:type  "mock-resend-aloitusheratteet"
               :start "2022-01-19"
               :end   "2022-02-02"}
              {:type  "mock-resend-paattoheratteet"
               :start "2022-01-19"
               :end   "2022-02-02"}]))
      (is
        (true?
          (tu/logs-contain?
            {:level :info
             :message "Lähetetty 123 aloitusviestiä ja 456 päättöviestiä"}))))))
