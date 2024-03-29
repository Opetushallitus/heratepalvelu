(ns oph.heratepalvelu.tep.EmailMuistutusHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.tep.EmailMuistutusHandler :as emh]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(use-fixtures :each tu/clear-logs-before-test)

(def test-send-reminder-email-result (atom {}))

(defn- mock-send-email [data] (reset! test-send-reminder-email-result data))

(deftest test-send-reminder-email
  (testing "Varmista, että send-reminder-email kutsuu send-email oikein"
    (with-redefs [oph.heratepalvelu.external.viestintapalvelu/send-email
                  mock-send-email]
      (let [nippu {:kyselylinkki "kysely.linkki/123"
                   :lahetysosoite "a@b.com"}
            oppilaitokset [{:en "Test Institution"
                            :fi "Testilaitos"
                            :sv "Testanstalt"}]
            expected {:subject (str "Muistutus-påminnelse-reminder: "
                                    "Työpaikkaohjaajakysely - "
                                    "Enkät till arbetsplatshandledaren - "
                                    "Survey to workplace instructors")
                      :body (vp/tyopaikkaohjaaja-muistutus-html nippu
                                                                oppilaitokset)
                      :address (:lahetysosoite nippu)
                      :sender "OPH – UBS – EDUFI"}]
        (emh/send-reminder-email nippu oppilaitokset)
        (is (= @test-send-reminder-email-result expected))))))

(def test-update-item-email-sent-result (atom {}))

(defn- mock-uies-update-nippu [nippu updates]
  (reset! test-update-item-email-sent-result {:nippu nippu :updates updates}))

(deftest test-update-item-email-sent
  (testing "Varmista, että update-item-email-sent kutsuu update-item oikein"
    (with-redefs
      [environ.core/env {:nippu-table "nippu-table-name"}
       oph.heratepalvelu.common/local-date-now (fn [] (LocalDate/of 2022 2 2))
       oph.heratepalvelu.tep.tepCommon/update-nippu mock-uies-update-nippu]
      (let [nippu {:ohjaaja_ytunnus_kj_tutkinto "test-nippu-id"
                   :niputuspvm "2021-10-05"}
            id 123
            expected {:nippu {:ohjaaja_ytunnus_kj_tutkinto "test-nippu-id"
                              :niputuspvm "2021-10-05"}
                      :updates {:kasittelytila
                                [:s (:viestintapalvelussa c/kasittelytilat)]
                                :viestintapalvelu-id [:n 123]
                                :email_muistutuspvm  [:s "2022-02-02"]
                                :muistutukset        [:n 1]}}]
        (emh/update-item-email-sent nippu id)
        (is (= @test-update-item-email-sent-result expected))))))

(def test-update-item-cannot-answer-result (atom {}))

(defn- mock-uica-update-nippu [nippu updates]
  (reset! test-update-item-cannot-answer-result
          {:nippu nippu :updates updates}))

(deftest test-update-item-cannot-answer
  (testing "Varmista, että update-item-cannot-answer kutsuu update-item oikein"
    (with-redefs
      [environ.core/env {:nippu-table "nippu-table-name"}
       oph.heratepalvelu.tep.tepCommon/update-nippu mock-uica-update-nippu]
      (let [nippu {:ohjaaja_ytunnus_kj_tutkinto "test-nippu-id"
                   :niputuspvm "2021-10-10"}
            status1 {:vastattu true}
            status2 {:vastattu false}
            expected1 {:nippu nippu
                       :updates {:kasittelytila
                                 [:s (:vastattu c/kasittelytilat)]
                                 :muistutukset [:n 1]}}
            expected2 {:nippu nippu
                       :updates {:kasittelytila
                                 [:s (:vastausaika-loppunut-m c/kasittelytilat)]
                                 :muistutukset [:n 1]}}]
        (emh/update-item-cannot-answer nippu status1)
        (is (= @test-update-item-cannot-answer-result expected1))
        (reset! test-update-item-cannot-answer-result {})
        (emh/update-item-cannot-answer nippu status2)
        (is (= @test-update-item-cannot-answer-result expected2))))))

(def test-sendEmailMuistutus-call-log (atom []))

(defn- add-to-test-sendEmailMuistutus-call-log [item]
  (reset! test-sendEmailMuistutus-call-log
          (cons item @test-sendEmailMuistutus-call-log)))

(defn- mock-get-nippulinkki-status [kyselylinkki]
  (add-to-test-sendEmailMuistutus-call-log (str "get-nippulinkki-status: "
                                                kyselylinkki))
  (if (= kyselylinkki "kysely.linkki/vastattu_QWERTY")
    {:vastattu true
     :voimassa_loppupvm "2021-10-12"}
    (if (= kyselylinkki "kysely.linkki/ei_vastattu_YUOIOP")
      {:vastattu false
       :voimassa_loppupvm "2021-10-12"}
      {:vastattu false
       :voimassa_loppupvm "2021-10-08"})))

(defn- mock-has-time-to-answer? [pvm]
  (add-to-test-sendEmailMuistutus-call-log (str "has-time-to-answer?: " pvm))
  (>= (compare pvm "2021-10-10") 0))

(defn- mock-get-jaksot-for-nippu [nippu]
  (add-to-test-sendEmailMuistutus-call-log (str "get-jaksot-for-nippu: " nippu))
  [{:kyselylinkki (:kyselylinkki nippu)}])

(defn- mock-get-oppilaitokset [jaksot]
  (add-to-test-sendEmailMuistutus-call-log (str "get-oppilaitokset: " jaksot))
  (seq [{:en "Test Institution"
         :fi "Testilaitos"
         :sv "Testanstalt"}]))

(defn- mock-send-reminder-email [nippu oppilaitokset]
  (add-to-test-sendEmailMuistutus-call-log
    (str "send-reminder-email: " nippu " " oppilaitokset))
  {:id (if (= (:kyselylinkki nippu) "kysely.linkki/vastattu_QWERTY")
         123
         (if (= (:kyselylinkki nippu) "kysely.linkki/ei_vastattu_YUOIOP")
           456
           789))})

(defn- mock-update-item-email-sent [nippu id]
  (add-to-test-sendEmailMuistutus-call-log
    (str "update-item-email-sent: " nippu " " id)))

(defn- mock-update-item-cannot-answer [nippu status]
  (add-to-test-sendEmailMuistutus-call-log
    (str "update-item-cannot-answer: " nippu " " status)))

(deftest test-sendEmailMuistutus
  (testing "Varmista, että sendEmailMuistutus kutsuu funktioita oikein"
    (with-redefs
      [clojure.tools.logging/log* tu/mock-log*
       oph.heratepalvelu.common/has-time-to-answer? mock-has-time-to-answer?
       oph.heratepalvelu.external.arvo/get-nippulinkki-status
       mock-get-nippulinkki-status
       oph.heratepalvelu.tep.EmailMuistutusHandler/send-reminder-email
       mock-send-reminder-email
       oph.heratepalvelu.tep.EmailMuistutusHandler/update-item-email-sent
       mock-update-item-email-sent
       oph.heratepalvelu.tep.EmailMuistutusHandler/update-item-cannot-answer
       mock-update-item-cannot-answer
       oph.heratepalvelu.tep.tepCommon/get-jaksot-for-nippu
       mock-get-jaksot-for-nippu
       oph.heratepalvelu.common/get-oppilaitokset
       mock-get-oppilaitokset]
      (let [muistutettavat [{:kyselylinkki "kysely.linkki/vastattu_QWERTY"}
                            {:kyselylinkki "kysely.linkki/ei_vastattu_YUOIOP"}
                            {:kyselylinkki "kysely.linkki/xyz_GHKJJK"}]

            expected-call-log
            ["get-nippulinkki-status: kysely.linkki/vastattu_QWERTY"
             (str "update-item-cannot-answer: "
                  "{:kyselylinkki \"kysely.linkki/vastattu_QWERTY\"} "
                  "{:vastattu true, :voimassa_loppupvm \"2021-10-12\"}")
             "get-nippulinkki-status: kysely.linkki/ei_vastattu_YUOIOP"
             "has-time-to-answer?: 2021-10-12"
             (str "get-jaksot-for-nippu: "
                  "{:kyselylinkki \"kysely.linkki/ei_vastattu_YUOIOP\"}")
             (str "get-oppilaitokset: "
                  "[{:kyselylinkki \"kysely.linkki/ei_vastattu_YUOIOP\"}]")
             (str "send-reminder-email: "
                  "{:kyselylinkki \"kysely.linkki/ei_vastattu_YUOIOP\"} "
                  "({:en \"Test Institution\", :fi \"Testilaitos\", "
                  ":sv \"Testanstalt\"})")
             (str "update-item-email-sent: "
                  "{:kyselylinkki \"kysely.linkki/ei_vastattu_YUOIOP\"} 456")
             "get-nippulinkki-status: kysely.linkki/xyz_GHKJJK"
             "has-time-to-answer?: 2021-10-08"
             (str "update-item-cannot-answer: "
                  "{:kyselylinkki \"kysely.linkki/xyz_GHKJJK\"} "
                  "{:vastattu false, :voimassa_loppupvm \"2021-10-08\"}")]]
        (emh/sendEmailMuistutus (fn [] false) muistutettavat)
        (is (= @test-sendEmailMuistutus-call-log (reverse expected-call-log)))
        (is (tu/logs-contain?
              {:level :info :message "Aiotaan käsitellä 3 muistutusta."}))))))

(defn- mock-query-items [query-params options table]
  (when (and (= :eq (first (:muistutukset query-params)))
             (= :n (first (second (:muistutukset query-params))))
             (= 0 (second (second (:muistutukset query-params))))
             (= :between (first (:lahetyspvm query-params)))
             (= :s (first (first (second (:lahetyspvm query-params)))))
             (= :s (first (second (second (:lahetyspvm query-params)))))
             (= "emailMuistutusIndex" (:index options))
             (= "nippu-table-name" table))
    {:start-date (second (first (second (:lahetyspvm query-params))))
     :end-date (second (second (second (:lahetyspvm query-params))))}))

(deftest test-query-muistutukset
  (testing "Varmista, että query-muistutukset kutsuu query-items oikein"
    (with-redefs [environ.core/env {:nippu-table "nippu-table-name"}
                  oph.heratepalvelu.db.dynamodb/query-items mock-query-items]
      (let [expected {:start-date (str (.minusDays (LocalDate/now) 10))
                      :end-date (str (.minusDays (LocalDate/now) 5))}]
        (is (= (emh/query-muistutukset) expected))))))

(def test-handleSendEmailMuistutus-result (atom {}))

(defn- mock-query-muistutukset [] [{:muistutus-contents "test-data"}])

(defn- mock-sendEmailMuistutus [_ muistutettavat]
  (reset! test-handleSendEmailMuistutus-result
          {:muistutettavat muistutettavat}))

(deftest test-handleSendEmailMuistutus
  (testing "Varmista, että -handleSendEmailMuistutus kutsuu funktioita oikein"
    (with-redefs [clojure.tools.logging/log* tu/mock-log*
                  oph.heratepalvelu.tep.EmailMuistutusHandler/query-muistutukset
                  mock-query-muistutukset
                  oph.heratepalvelu.tep.EmailMuistutusHandler/sendEmailMuistutus
                  mock-sendEmailMuistutus]
      (let [event (tu/mock-handler-event :scheduledherate)
            context (tu/mock-handler-context)
            expected {:muistutettavat [{:muistutus-contents "test-data"}]}]
        (emh/-handleSendEmailMuistutus {} event context)
        (is (= @test-handleSendEmailMuistutus-result expected))))))
