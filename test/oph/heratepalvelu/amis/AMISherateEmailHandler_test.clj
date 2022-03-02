(ns oph.heratepalvelu.amis.AMISherateEmailHandler-test
  (:require [clj-time.core :as t]
            [clojure.test :refer :all]
            [oph.heratepalvelu.amis.AMISherateEmailHandler :as heh]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(def mock-update-herate-result (atom {}))

(defn- mock-update-herate [herate updates]
  (reset! mock-update-herate-result {:herate herate :updates updates}))

(deftest test-save-email-to-db
  (testing "Varmista, että same-email-to-db kutsuu update-herate oikein"
    (with-redefs [oph.heratepalvelu.amis.AMISCommon/update-herate
                  mock-update-herate]
      (let [email {:toimija_oppija "toimija-oppija"
                   :tyyppi_kausi "tyyppi-kausi"}
            id "asdfasfasfd"
            lahetyspvm "2021-04-04"
            expected {:herate {:toimija_oppija "toimija-oppija"
                               :tyyppi_kausi "tyyppi-kausi"}
                      :updates {:lahetystila
                                [:s (:viestintapalvelussa c/kasittelytilat)]
                                :viestintapalvelu-id [:n id]
                                :lahetyspvm [:s lahetyspvm]
                                :muistutukset [:n 0]}}]
        (heh/save-email-to-db email id lahetyspvm)
        (is (= @mock-update-herate-result expected))))))

(def mock-send-lahetys-data-to-ehoks-result (atom {}))

(defn- mock-send-lahetys-data-to-ehoks [toimija-oppija tyyppi-kausi data]
  (reset! mock-send-lahetys-data-to-ehoks-result
          {:toimija-oppija toimija-oppija
           :tyyppi-kausi tyyppi-kausi
           :kyselylinkki (:kyselylinkki data)
           :lahetyspvm (:lahetyspvm data)
           :sahkoposti (:sahkoposti data)
           :lahetystila (:lahetystila data)}))

(deftest test-update-data-in-ehoks
  (testing (str "Varmista, että update-data-in-ehoks kutsuu "
                "send-lahetys-data-to-ehoks oikein")
    (with-redefs [oph.heratepalvelu.common/send-lahetys-data-to-ehoks
                  mock-send-lahetys-data-to-ehoks]
      (let [email {:toimija_oppija "toimija-oppija"
                   :tyyppi_kausi "tyyppi-kausi"
                   :kyselylinkki "kysely.linkki/1324"
                   :sahkoposti "a@b.com"}
            lahetyspvm "2021-10-10"
            expected {:toimija-oppija "toimija-oppija"
                      :tyyppi-kausi "tyyppi-kausi"
                      :kyselylinkki "kysely.linkki/1324"
                      :lahetyspvm lahetyspvm
                      :sahkoposti "a@b.com"
                      :lahetystila (:viestintapalvelussa c/kasittelytilat)}]
        (heh/update-data-in-ehoks email lahetyspvm)
        (is (= @mock-send-lahetys-data-to-ehoks-result expected))))))

(def mock-send-email-result (atom {}))

(defn- mock-send-email [data]
  (reset! mock-send-email-result {:subject (:subject data)
                                  :body (:body data)
                                  :address (:address data)
                                  :sender (:sender data)}))

(deftest test-send-feedback-email
  (testing "Varmista, että palautesähköposti lähetetään oikein"
    (with-redefs [oph.heratepalvelu.external.viestintapalvelu/send-email
                  mock-send-email]
      (let [email {:sahkoposti "a@b.com"
                   :suorituskieli "fi"
                   :kyselytyyppi "aloittaneet"
                   :kyselylinkki "kysely.linkki/1234"}
            expected {:subject (str "Palautetta oppilaitokselle - "
                                    "Respons till läroanstalten - "
                                    "Feedback to educational institution")
                      :body (vp/amispalaute-html email)
                      :address (:sahkoposti email)
                      :sender "Opetushallitus – Utbildningsstyrelsen – EDUFI"}]
        (is (= expected (heh/send-feedback-email email)))))))

(def mock-no-time-to-answer-update-herate-result (atom {}))

(defn- mock-no-time-to-answer-update-herate [herate updates]
  (reset! mock-no-time-to-answer-update-herate-result {:herate herate
                                                       :updates updates}))

(deftest test-save-no-time-to-answer
  (testing "Varmista, että save-no-time-to-answer kutsuu update-herate oikein"
    (with-redefs [oph.heratepalvelu.amis.AMISCommon/update-herate
                  mock-no-time-to-answer-update-herate
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/parse "2021-10-10"))]
      (let [email {:toimija_oppija "toimija-oppija"
                   :tyyppi_kausi "tyyppi-kausi"}
            expected {:herate email
                      :updates {:lahetystila
                                [:s (:vastausaika-loppunut c/kasittelytilat)]
                                :lahetyspvm [:s "2021-10-10"]}}]
        (heh/save-no-time-to-answer email)
        (is (= @mock-no-time-to-answer-update-herate-result expected))))))

(defn- mock-query-items [query-params options]
  (and (= :eq (first (:lahetystila query-params)))
       (= :s (first (second (:lahetystila query-params))))
       (= (:ei-lahetetty c/kasittelytilat)
          (second (second (:lahetystila query-params))))
       (= :le (first (:alkupvm query-params)))
       (= :s (first (second (:alkupvm query-params))))
       (= "2021-10-10" (second (second (:alkupvm query-params))))
       (= "lahetysIndex" (:index options))
       (= 10 (:limit options))))

(deftest test-do-query
  (testing "Varmista, että do-query kutsuu query-items oikein"
    (with-redefs [oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/parse "2021-10-10"))
                  oph.heratepalvelu.db.dynamodb/query-items mock-query-items]
      (is (true? (heh/do-query))))))

(defn- mock-do-query [] [{:kyselylinkki "good.kyselylinkki/123"}])
(defn- mock-do-query-no-time [] [{:kyselylinkki "notime.kyselylinkki/123"}])

(defn- mock-get-kyselylinkki-status [kyselylinkki]
  (if (= kyselylinkki "notime.kyselylinkki/123")
    {:voimassa_loppupvm "2021-10-10"}
    {:voimassa_loppupvm "2025-10-10"}))

(defn- mock-has-time-to-answer? [pvm] (> (compare pvm "2021-12-12") 0))
(defn- mock-send-feedback-email [email] {:id "test-id"})

(def general-results (atom ""))

(defn- mock-save-email-to-db [email id lahetyspvm]
  (reset! general-results (str @general-results
                               "save: "
                               (:kyselylinkki email)
                               " "
                               id
                               " "
                               lahetyspvm
                               "; ")))

(defn- mock-update-data-in-ehoks [email lahetyspvm]
  (reset! general-results (str @general-results
                               "ehoks: "
                               (:kyselylinkki email)
                               " "
                               lahetyspvm)))

(defn- mock-save-no-time-to-answer [email]
  (reset! general-results
          (str @general-results "Ei aikaa: " (:kyselylinkki email))))

(deftest test-handleSendAMISEmails
  (testing "Varmista, että -handleSendAMISEmails toimii oikein"
    (with-redefs
      [oph.heratepalvelu.amis.AMISherateEmailHandler/save-email-to-db
       mock-save-email-to-db
       oph.heratepalvelu.amis.AMISherateEmailHandler/update-data-in-ehoks
       mock-update-data-in-ehoks
       oph.heratepalvelu.amis.AMISherateEmailHandler/save-no-time-to-answer
       mock-save-no-time-to-answer
       oph.heratepalvelu.amis.AMISherateEmailHandler/send-feedback-email
       mock-send-feedback-email
       oph.heratepalvelu.common/has-time-to-answer? mock-has-time-to-answer?
       oph.heratepalvelu.common/local-date-now
       (fn [] (LocalDate/of 2025 10 10))
       oph.heratepalvelu.external.arvo/get-kyselylinkki-status
       mock-get-kyselylinkki-status]
      (let [event (tu/mock-handler-event :scheduledherate)
            context (tu/mock-handler-context)]
        (with-redefs
          [oph.heratepalvelu.amis.AMISherateEmailHandler/do-query mock-do-query]
          (heh/-handleSendAMISEmails {} event context)
          (is (= @general-results
                 (str "save: good.kyselylinkki/123 test-id 2025-10-10; "
                      "ehoks: good.kyselylinkki/123 2025-10-10"))))
        (reset! general-results "")
        (with-redefs [oph.heratepalvelu.amis.AMISherateEmailHandler/do-query
                      mock-do-query-no-time]
          (heh/-handleSendAMISEmails {} event context)
          (is (= @general-results "Ei aikaa: notime.kyselylinkki/123")))))))
