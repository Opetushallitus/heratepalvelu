(ns oph.heratepalvelu.tep.SMSMuistutusHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.elisa :as elisa]
            [oph.heratepalvelu.tep.SMSMuistutusHandler :as smh]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(def test-sendSmsMuistutus-results (atom []))

(defn- add-to-test-sendSmsMuistutus-results [data]
  (reset! test-sendSmsMuistutus-results
          (cons data @test-sendSmsMuistutus-results)))

(defn- mock-has-time-to-answer? [pvm] (<= 0 (compare pvm "2021-12-16")))

(defn- mock-get-nippulinkki-status [kyselylinkki]
  (add-to-test-sendSmsMuistutus-results {:type "mock-get-nippulinkki-status"
                                         :kyselylinkki kyselylinkki})
  (if (= kyselylinkki "kysely.linkki/1")
    {:vastattu false :voimassa_loppupvm "2021-12-30"}
    (if (= kyselylinkki "kysely.linkki/2")
      {:vastattu true :voimassa_loppupvm "2021-12-30"}
      {:vastattu false :voimassa_loppupvm "2021-12-10"})))

(defn- mock-get-jaksot-for-nippu [nippu]
  (add-to-test-sendSmsMuistutus-results {:type "mock-get-jaksot-for-nippu"
                                         :nippu nippu})
  [{:oppilaitos "1234"}])

(defn- mock-get-organisaatio [oppilaitos]
  (add-to-test-sendSmsMuistutus-results {:type "mock-get-organisaatio"
                                         :oppilaitos oppilaitos})
  {:nimi {:fi "Testilaitos" :en "Test Dept." :sv "Testanstalt"}})

(defn- mock-send-tep-sms [numero body]
  (add-to-test-sendSmsMuistutus-results {:type "mock-send-tep-sms"
                                         :numero numero
                                         :body body})
  {:body {:messages {(keyword numero) {:status "mock-lahetys"}}}})

(defn- mock-update-nippu [nippu updates]
  (add-to-test-sendSmsMuistutus-results {:type "mock-update-nippu"
                                         :nippu nippu
                                         :updates updates}))

(deftest test-sendSmsMuistutus
  (testing "Varmista, että sendSmsMuistutus kutsuu oikeita funktioita"
    (with-redefs [environ.core/env {:nippu-table "nippu-table-name"}
                  oph.heratepalvelu.common/has-time-to-answer?
                  mock-has-time-to-answer?
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2021 12 16))
                  oph.heratepalvelu.external.arvo/get-nippulinkki-status
                  mock-get-nippulinkki-status
                  oph.heratepalvelu.external.elisa/send-tep-sms
                  mock-send-tep-sms
                  oph.heratepalvelu.external.organisaatio/get-organisaatio
                  mock-get-organisaatio
                  oph.heratepalvelu.tep.tepCommon/get-jaksot-for-nippu
                  mock-get-jaksot-for-nippu
                  oph.heratepalvelu.tep.tepCommon/update-nippu
                  mock-update-nippu]
      (let [muistutettavat [{:kyselylinkki "kysely.linkki/1"
                             :ohjaaja_ytunnus_kj_tutkinto "test-id-1"
                             :niputuspvm "2021-12-15"
                             :lahetettynumeroon "+358401234567"}
                            {:kyselylinkki "kysely.linkki/2"
                             :ohjaaja_ytunnus_kj_tutkinto "test-id-2"
                             :niputuspvm "2021-12-15"
                             :lahetettynumeroon "+358401234567"}
                            {:kyselylinkki "kysely.linkki/3"
                             :ohjaaja_ytunnus_kj_tutkinto "test-id-3"
                             :niputuspvm "2021-12-15"
                             :lahetettynumeroon "+358401234567"}]
            results [{:type "mock-get-nippulinkki-status"
                      :kyselylinkki "kysely.linkki/1"}
                     {:type "mock-get-jaksot-for-nippu"
                      :nippu {:kyselylinkki "kysely.linkki/1"
                              :ohjaaja_ytunnus_kj_tutkinto "test-id-1"
                              :niputuspvm "2021-12-15"
                              :lahetettynumeroon "+358401234567"}}
                     {:type "mock-get-organisaatio"
                      :oppilaitos "1234"}
                     {:type "mock-send-tep-sms"
                      :numero "+358401234567"
                      :body (elisa/muistutus-msg-body "kysely.linkki/1"
                                                      [{:fi "Testilaitos"
                                                        :en "Test Dept."
                                                        :sv "Testanstalt"}])}
                     {:type "mock-update-nippu"
                      :nippu {:kyselylinkki "kysely.linkki/1"
                              :ohjaaja_ytunnus_kj_tutkinto "test-id-1"
                              :niputuspvm "2021-12-15"
                              :lahetettynumeroon "+358401234567"}
                      :updates {:sms_kasittelytila [:s "mock-lahetys"]
                                :sms_muistutuspvm [:s "2021-12-16"]
                                :sms_muistutukset [:n 1]}}
                     {:type "mock-get-nippulinkki-status"
                      :kyselylinkki "kysely.linkki/2"}
                     {:type "mock-update-nippu"
                      :nippu {:kyselylinkki "kysely.linkki/2"
                              :ohjaaja_ytunnus_kj_tutkinto "test-id-2"
                              :niputuspvm "2021-12-15"
                              :lahetettynumeroon "+358401234567"}
                      :updates {:sms_kasittelytila
                                [:s (:vastattu c/kasittelytilat)]
                                :sms_muistutukset [:n 1]}}
                     {:type "mock-get-nippulinkki-status"
                      :kyselylinkki "kysely.linkki/3"}
                     {:type "mock-update-nippu"
                      :nippu {:kyselylinkki "kysely.linkki/3"
                              :ohjaaja_ytunnus_kj_tutkinto "test-id-3"
                              :niputuspvm "2021-12-15"
                              :lahetettynumeroon "+358401234567"}
                      :updates {:sms_kasittelytila
                                [:s (:vastausaika-loppunut-m c/kasittelytilat)]
                                :sms_muistutukset [:n 1]}}]]
        (smh/sendSmsMuistutus muistutettavat)
        (is (= results (vec (reverse @test-sendSmsMuistutus-results))))))))

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
