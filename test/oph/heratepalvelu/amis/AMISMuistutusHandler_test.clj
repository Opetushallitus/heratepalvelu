(ns oph.heratepalvelu.amis.AMISMuistutusHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.amis.AMISMuistutusHandler :as mh]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(def mock-update-after-send-results (atom {}))

(defn- mock-update-herate-for-update-after-send [herate updates]
  (reset! mock-update-after-send-results {:herate herate :updates updates}))

(deftest test-update-after-send
  (testing "Varmista, että update-after-send kutsuu update-item oikein"
    (with-redefs [oph.heratepalvelu.amis.AMISCommon/update-herate
                  mock-update-herate-for-update-after-send
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2021 10 10))]
      (let [email {:toimija_oppija "toimija-oppija"
                   :tyyppi_kausi "tyyppi-kausi"}
            expected {:herate email
                      :updates {:muistutukset [:n 1]
                                :viestintapalvelu-id [:n 123]
                                :1.-muistutus-lahetetty [:s "2021-10-10"]
                                :lahetystila
                                [:s (:viestintapalvelussa c/kasittelytilat)]}}]
        (mh/update-after-send email 1 123)
        (is (= @mock-update-after-send-results expected))))))

(def mock-update-when-not-sent-results (atom {}))

(defn- mock-update-herate-for-update-when-not-sent [herate updates]
  (reset! mock-update-when-not-sent-results {:herate herate :updates updates}))

(deftest test-update-when-not-sent
  (testing "Varmista, että update-when-not-sent kutsuu update-item oikein"
    (with-redefs [oph.heratepalvelu.amis.AMISCommon/update-herate
                  mock-update-herate-for-update-when-not-sent]
      (let [email {:toimija_oppija "toimija-oppija"
                   :tyyppi_kausi "tyyppi-kausi"}
            expected {:muistutukset [:n 1]}
            expected-vastattu (assoc expected
                                     :lahetystila
                                     [:s (:vastattu c/kasittelytilat)])
            expected-aika-loppunut
            (assoc expected
                   :lahetystila
                   [:s (:vastausaika-loppunut-m c/kasittelytilat)])]
        (mh/update-when-not-sent email 1 {:vastattu true})
        (is (= @mock-update-when-not-sent-results {:herate email
                                                   :updates expected-vastattu}))
        (mh/update-when-not-sent email 1 {:vastattu false})
        (is (= @mock-update-when-not-sent-results
               {:herate email :updates expected-aika-loppunut}))))))

(def mock-send-email-results (atom {}))

(defn- mock-send-email [options] (reset! mock-send-email-results options))

(deftest test-send-reminder-email
  (testing "Varmista, että send-reminder-email kutsuu send-email oikein"
    (with-redefs [oph.heratepalvelu.external.viestintapalvelu/send-email
                  mock-send-email]
      (let [email {:sahkoposti "a@b.com"
                   :suorituskieli "fi"
                   :kyselytyyppi "aloittaneet"
                   :kyselylinkki "kysely.linkki/123"}
            expected {:subject (str "Muistutus-påminnelse-reminder: "
                                    "Vastaa kyselyyn - svara på enkäten - "
                                    "answer the survey")
                      :body (vp/amismuistutus-html email)
                      :address (:sahkoposti email)
                      :sender "Opetushallitus – Utbildningsstyrelsen – EDUFI"}]
        (mh/send-reminder-email email)
        (is (= @mock-send-email-results expected))))))

(def test-sendAMISMuistutus-results (atom ""))

(defn- mock-get-kyselylinkki-status [kyselylinkki]
  (cond
    (= kyselylinkki "kysely.linkki/1")
    {:vastattu false
     :voimassa_loppupvm "2021-12-12"}
    (= kyselylinkki "kysely.linkki/2")
    {:vastattu false
     :voimassa_loppupvm "2021-10-10"}
    (= kyselylinkki "kysely.linkki/3")
    {:vastattu true
     :voimassa_loppupvm "2021-12-12"}
    :else
    {:vastattu true
     :voimassa_loppupvm "2021-10-10"}))

(defn- mock-has-time-to-answer? [pvm] (>= (compare pvm "2021-11-11") 0))

(defn- mock-send-reminder-email [email]
  (reset! test-sendAMISMuistutus-results
          (str @test-sendAMISMuistutus-results "reminder-email " email " "))
  {:id "testid"})

(defn- mock-update-after-send [email n id]
  (reset! test-sendAMISMuistutus-results
          (str @test-sendAMISMuistutus-results email " " n " " id " ")))

(defn- mock-update-when-not-sent [email n status]
  (reset! test-sendAMISMuistutus-results
          (str @test-sendAMISMuistutus-results email " " n " " status " ")))

(deftest test-sendAMISMuistutus
  (testing "Varmista, että sendAMISMuistutus kutsuu muita funktioita oikein"
    (with-redefs
      [oph.heratepalvelu.amis.AMISMuistutusHandler/send-reminder-email
       mock-send-reminder-email
       oph.heratepalvelu.amis.AMISMuistutusHandler/update-after-send
       mock-update-after-send
       oph.heratepalvelu.amis.AMISMuistutusHandler/update-when-not-sent
       mock-update-when-not-sent
       oph.heratepalvelu.common/has-time-to-answer? mock-has-time-to-answer?
       oph.heratepalvelu.external.arvo/get-kyselylinkki-status
       mock-get-kyselylinkki-status]
      (let [muistutettavat1 [{:kyselylinkki "kysely.linkki/1"}]
            expected1 (str "reminder-email {:kyselylinkki \"kysely.linkki/1\"} "
                           "{:kyselylinkki \"kysely.linkki/1\"} 1 testid ")
            muistutettavat2 [{:kyselylinkki "kysely.linkki/2"}]
            expected2 (str "{:kyselylinkki \"kysely.linkki/2\"} 1 "
                           "{:vastattu false, "
                           ":voimassa_loppupvm \"2021-10-10\"} ")
            muistutettavat3 [{:kyselylinkki "kysely.linkki/3"}]
            expected3 (str "{:kyselylinkki \"kysely.linkki/3\"} 1 "
                           "{:vastattu true, "
                           ":voimassa_loppupvm \"2021-12-12\"} ")
            muistutettavat4 [{:kyselylinkki "kysely.linkki/4"}]
            expected4 (str "{:kyselylinkki \"kysely.linkki/4\"} 1 "
                           "{:vastattu true, "
                           ":voimassa_loppupvm \"2021-10-10\"} ")]
        (mh/sendAMISMuistutus muistutettavat1 1)
        (is (= @test-sendAMISMuistutus-results expected1))
        (reset! test-sendAMISMuistutus-results "")
        (mh/sendAMISMuistutus muistutettavat2 1)
        (is (= @test-sendAMISMuistutus-results expected2))
        (reset! test-sendAMISMuistutus-results "")
        (mh/sendAMISMuistutus muistutettavat3 1)
        (is (= @test-sendAMISMuistutus-results expected3))
        (reset! test-sendAMISMuistutus-results "")
        (mh/sendAMISMuistutus muistutettavat4 1)
        (is (= @test-sendAMISMuistutus-results expected4))))))

(def mock-query-items-results (atom {}))

(defn- mock-query-items [query-params options]
  (when (and (= :eq (first (:muistutukset query-params)))
             (= :n (first (second (:muistutukset query-params))))
             (= :between (first (:lahetyspvm query-params)))
             (= :s (first (first (second (:lahetyspvm query-params)))))
             (= :s (first (second (second (:lahetyspvm query-params)))))
             (= "muistutusIndex" (:index options)))
    (reset! mock-query-items-results
            {:muistutukset (second (second (:muistutukset query-params)))
             :start-span (second (first (second (:lahetyspvm query-params))))
             :end-span (second (second (second (:lahetyspvm query-params))))})))

(deftest test-query-muistutukset
  (testing "Varmista, että query-muistutukset kutsuu query-items oikein"
    (with-redefs
      [oph.heratepalvelu.common/local-date-now (fn [] (LocalDate/of 2021 10 10))
       oph.heratepalvelu.db.dynamodb/query-items mock-query-items]
      (let [expected-1 {:muistutukset 0
                        :start-span "2021-10-01"
                        :end-span "2021-10-05"}
            expected-2 {:muistutukset 1
                        :start-span "2021-09-26"
                        :end-span "2021-09-30"}]
        (mh/query-muistutukset 1)
        (is (= @mock-query-items-results expected-1))
        (mh/query-muistutukset 2)
        (is (= @mock-query-items-results expected-2))))))

(def test-handleSendAMISMuistutus-results (atom ""))

(defn- mock-query-muistutukset [n] {:fake-muistutus-level n})

(defn- mock-sendAMISMuistutus [muistutettavat n]
  (when (= n (:fake-muistutus-level muistutettavat))
    (reset!
      test-handleSendAMISMuistutus-results
      (str @test-handleSendAMISMuistutus-results "muistutus-level " n " "))))

(deftest test-handleSendAMISMuistutus
  (testing "Varmista, että -handleSendAMISMuistutus kutsuu funktioita oikein"
    (with-redefs [oph.heratepalvelu.amis.AMISMuistutusHandler/query-muistutukset
                  mock-query-muistutukset
                  oph.heratepalvelu.amis.AMISMuistutusHandler/sendAMISMuistutus
                  mock-sendAMISMuistutus]
      (let [event (tu/mock-handler-event :scheduledherate)
            context (tu/mock-handler-context)
            expected "muistutus-level 1 muistutus-level 2 "]
        (mh/-handleSendAMISMuistutus {} event context)
        (is (= @test-handleSendAMISMuistutus-results expected))))))
