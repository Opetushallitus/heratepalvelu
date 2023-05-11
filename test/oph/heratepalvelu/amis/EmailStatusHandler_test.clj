(ns oph.heratepalvelu.amis.EmailStatusHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.amis.EmailStatusHandler :as esh]
            [oph.heratepalvelu.test-util :as tu]))

(def mock-send-lahetys-data-to-ehoks-results (atom {}))

(defn- mock-send-lahetys-data-to-ehoks [toimija-oppija tyyppi-kausi data]
  (reset! mock-send-lahetys-data-to-ehoks-results
          (assoc data
                 :toimija-oppija toimija-oppija
                 :tyyppi-kausi tyyppi-kausi)))

(defn- mock-get-item [query-params]
  (when (and (= :s (first (:toimija_oppija query-params)))
             (= :s (first (:tyyppi_kausi query-params))))
    (cond
      (and (= "toimija-oppija-muistutukset"
              (second (:toimija_oppija query-params)))
           (= "tyyppi-kausi" (second (:tyyppi_kausi query-params))))
      {:muistutukset 1}
      (and (= "toimija-oppija-ei-muistutukset"
              (second (:toimija_oppija query-params)))
           (= "tyyppi-kausi" (second (:tyyppi_kausi query-params))))
      {:muistutukset 0}
      :else nil)))

(deftest test-update-ehoks-if-not-muistutus!
  (testing "Varmista, että update-ehoks-if-not-muistutus! tekee kutsuja oikein"
    (with-redefs [oph.heratepalvelu.common/send-lahetys-data-to-ehoks
                  mock-send-lahetys-data-to-ehoks
                  oph.heratepalvelu.db.dynamodb/get-item mock-get-item]
      (let [email1 {:toimija_oppija "toimija-oppija-muistutukset"
                    :tyyppi_kausi "tyyppi-kausi"
                    :kyselylinkki "kysely.linkki/123"
                    :sahkoposti "a@b.com"}
            email2 {:toimija_oppija "toimija-oppija-ei-muistutukset"
                    :tyyppi_kausi "tyyppi-kausi"
                    :kyselylinkki "kysely.linkki/123"
                    :sahkoposti "a@b.com"}
            status {:sendingEnded "2021-10-10T23:45:34"}
            tila (:viestintapalvelussa c/kasittelytilat)
            expected1 {}
            expected2 {:toimija-oppija "toimija-oppija-ei-muistutukset"
                       :tyyppi-kausi "tyyppi-kausi"
                       :kyselylinkki "kysely.linkki/123"
                       :lahetyspvm "2021-10-10"
                       :sahkoposti "a@b.com"
                       :lahetystila (:viestintapalvelussa c/kasittelytilat)}]
        (esh/update-ehoks-if-not-muistutus! email1 status tila)
        (is (= @mock-send-lahetys-data-to-ehoks-results expected1))
        (esh/update-ehoks-if-not-muistutus! email2 status tila)
        (is (= @mock-send-lahetys-data-to-ehoks-results expected2))))))

(def mock-update-herate-results (atom {}))

(defn- mock-update-herate [herate updates]
  (reset! mock-update-herate-results {:herate herate :updates updates}))

(deftest test-update-db-tila!
  (testing "Varmista, että update-db-tila! kutsuu update-item oikein"
    (with-redefs [oph.heratepalvelu.amis.AMISCommon/update-herate
                  mock-update-herate]
      (let [email {:toimija_oppija "toimija-oppija"
                   :tyyppi_kausi "tyyppi-kausi"}
            tila (:success c/kasittelytilat)
            expected {:herate email
                      :updates {:lahetystila [:s (:success c/kasittelytilat)]}}]
        (esh/update-db-tila! email tila)
        (is (= @mock-update-herate-results expected))))))

(def mock-query-items-results (atom {}))

(defn- mock-query-items [query-params options]
  (reset! mock-query-items-results
          (and (= :eq (first (:lahetystila query-params)))
               (= :s (first (second (:lahetystila query-params))))
               (= (:viestintapalvelussa c/kasittelytilat)
                  (second (second (:lahetystila query-params))))
               (= "lahetysIndex" (:index options))
               (= 10 (:limit options)))))

(deftest test-do-query!
  (testing "Varmista, että do-query! kutsuu query-items oikein"
    (with-redefs [oph.heratepalvelu.db.dynamodb/query-items mock-query-items]
      (esh/do-query!)
      (is (true? @mock-query-items-results)))))

(def test-handleEmailStatus-results (atom ""))

(defn- mock-do-query [] [{:viestintapalvelu-id "12345"
                          :kyselylinkki "kysely.linkki/123"}])

(defn- mock-get-email-status [_] {:numberOfSuccessfulSendings 1})

(defn- mock-get-email-status-none [_] {})

(defn- mock-patch-kyselylinkki-metadata [kyselylinkki tila]
  (reset! test-handleEmailStatus-results
          (str @test-handleEmailStatus-results kyselylinkki " " tila " ")))

(defn- mock-update-ehoks-if-not-muistutus [email status tila]
  (reset! test-handleEmailStatus-results
          (str @test-handleEmailStatus-results email " " status " " tila " ")))

(defn- mock-update-db [email tila]
  (reset! test-handleEmailStatus-results
          (str @test-handleEmailStatus-results "update-db " email " " tila)))

(deftest test-handleEmailStatus
  (testing "Varmista, että -handleEmailStatus kutsuu muita funktioita oikein"
    (with-redefs
      [oph.heratepalvelu.amis.EmailStatusHandler/do-query! mock-do-query
       oph.heratepalvelu.amis.EmailStatusHandler/update-db-tila! mock-update-db
       oph.heratepalvelu.amis.EmailStatusHandler/update-ehoks-if-not-muistutus!
       mock-update-ehoks-if-not-muistutus
       oph.heratepalvelu.external.arvo/patch-kyselylinkki-metadata
       mock-patch-kyselylinkki-metadata]
      (let [event (tu/mock-handler-event :scheduledherate)
            context (tu/mock-handler-context)]
        (with-redefs
          [oph.heratepalvelu.external.viestintapalvelu/get-email-status
           mock-get-email-status]
          (esh/-handleEmailStatus {} event context)
          (is (= @test-handleEmailStatus-results
                 (str "kysely.linkki/123 lahetetty "
                      "{:viestintapalvelu-id \"12345\", "
                      ":kyselylinkki \"kysely.linkki/123\"} "
                      "{:numberOfSuccessfulSendings 1} lahetetty "
                      "update-db {:viestintapalvelu-id \"12345\", "
                      ":kyselylinkki \"kysely.linkki/123\"} lahetetty"))))
        (reset! test-handleEmailStatus-results "")
        (with-redefs
          [oph.heratepalvelu.external.viestintapalvelu/get-email-status
           mock-get-email-status-none]
          (esh/-handleEmailStatus {} event context)
          (is (= @test-handleEmailStatus-results "")))))))
