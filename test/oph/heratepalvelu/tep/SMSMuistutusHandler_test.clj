(ns oph.heratepalvelu.tep.SMSMuistutusHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.tep.SMSMuistutusHandler :as smh]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(defn- mock-query-muistutukset-query-items [query-params options table]
  (and (= :eq (first (:sms_muistutukset query-params)))
       (= :n (first (second (:sms_muistutukset query-params))))
       (= 0 (second (second (:sms_muistutukset query-params))))
       (= :between (first (:sms_lahetyspvm query-params)))
       (= :s (first (first (second (:sms_lahetyspvm query-params)))))
       (= "2021-12-05" (second (first (second (:sms_lahetyspvm query-params)))))
       (= :s (first (second (second (:sms_lahetyspvm query-params)))))
       (= "2021-12-10"
          (second (second (second (:sms_lahetyspvm query-params)))))
       (= "smsMuistutusIndex" (:index options))
       (= 10 (:limit options))
       (= "nippu-table-name" table)))

(deftest test-query-muistutukset
  (testing "Varmista, että query-muistutukset kutsuu query-items oikein"
    (with-redefs [environ.core/env {:nippu-table "nippu-table-name"}
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2021 12 15))
                  oph.heratepalvelu.db.dynamodb/query-items
                  mock-query-muistutukset-query-items]
      (is (true? (smh/query-muistutukset))))))

(def test-handleSendSMSMuistutus-results (atom []))

(defn- mock-query-muistutukset []
  (reset! test-handleSendSMSMuistutus-results
          (cons {:type "mock-query-muistutukset"}
                @test-handleSendSMSMuistutus-results))
  [{:type "Muistutettava"}])

(defn- mock-sendSmsMuistutus [muistutettavat]
  (reset! test-handleSendSMSMuistutus-results
          (cons {:type "mock-sendSmsMuistutus"
                 :value muistutettavat}
                @test-handleSendSMSMuistutus-results)))

(deftest test-handleSendSMSMuistutus
  (testing "Varmista, että -handleSendSMSMuistutus tekee kutsujaan oikein"
    (with-redefs [oph.heratepalvelu.tep.SMSMuistutusHandler/query-muistutukset
                  mock-query-muistutukset
                  oph.heratepalvelu.tep.SMSMuistutusHandler/sendSmsMuistutus
                  mock-sendSmsMuistutus]
      (let [event (tu/mock-handler-event :scheduledherate)
            context (tu/mock-handler-context)
            results [{:type "mock-query-muistutukset"}
                     {:type "mock-sendSmsMuistutus"
                      :value [{:type "Muistutettava"}]}]]
        (smh/-handleSendSMSMuistutus {} event context)
        (is (= results
               (vec (reverse @test-handleSendSMSMuistutus-results))))))))
