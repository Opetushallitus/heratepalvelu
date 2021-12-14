(ns oph.heratepalvelu.amis.EmailStatusHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.amis.EmailStatusHandler :as esh]))

(deftest test-convert-vp-email-status
  (testing "Converts viestintäpalvelu email status to internal tila"
    (let [status1 {:numberOfSuccessfulSendings 1
                   :numberOfBouncedSendings 0
                   :numberOfFailedSendings 0}
          status2 {:numberOfSuccessfulSendings 0
                   :numberOfBouncedSendings 1
                   :numberOfFailedSendings 0}
          status3 {:numberOfSuccessfulSendings 0
                   :numberOfBouncedSendings 0
                   :numberOfFailedSendings 1}
          status4 {:numberOfSuccessfulSendings 0
                   :numberOfBouncedSendings 0
                   :numberOfFailedSendings 0}]
      (is (= (:success c/kasittelytilat) (esh/convert-vp-email-status status1)))
      (is (= (:bounced c/kasittelytilat) (esh/convert-vp-email-status status2)))
      (is (= (:failed c/kasittelytilat) (esh/convert-vp-email-status status3)))
      (is (= nil (esh/convert-vp-email-status status4))))))

(def mock-send-lahetys-data-to-ehoks-results (atom {}))

(defn- mock-send-lahetys-data-to-ehoks [toimija-oppija tyyppi-kausi data]
  (reset! mock-send-lahetys-data-to-ehoks-results
          (assoc data :toimija-oppija toimija-oppija
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

(deftest test-update-ehoks-if-not-muistutus
  (testing "Varmista, että update-ehoks-if-not-muistutus tekee kutsuja oikein"
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
        (esh/update-ehoks-if-not-muistutus email1 status tila)
        (is (= @mock-send-lahetys-data-to-ehoks-results expected1))
        (esh/update-ehoks-if-not-muistutus email2 status tila)
        (is (= @mock-send-lahetys-data-to-ehoks-results expected2))))))

(def mock-update-item-results (atom {}))

(defn- mock-update-item [query-params options]
  (when (and (= :s (first (:toimija_oppija query-params)))
             (= :s (first (:tyyppi_kausi query-params)))
             (= :s (first (get (:expr-attr-vals options) ":lahetystila"))))
    (reset! mock-update-item-results
            {:toimija-oppija (second (:toimija_oppija query-params))
             :tyyppi-kausi (second (:tyyppi_kausi query-params))
             :lahetystila (second (get (:expr-attr-vals options)
                                       ":lahetystila"))})))

(deftest test-update-db
  (testing "Varmista, että update-db kutsuu update-item oikein"
    (with-redefs [oph.heratepalvelu.db.dynamodb/update-item mock-update-item]
      (let [email {:toimija_oppija "toimija-oppija"
                   :tyyppi_kausi "tyyppi-kausi"}
            tila (:success c/kasittelytilat)
            expected {:toimija-oppija "toimija-oppija"
                      :tyyppi-kausi "tyyppi-kausi"
                      :lahetystila (:success c/kasittelytilat)}]
        (esh/update-db email tila)
        (is (= @mock-update-item-results expected))))))

(def mock-query-items-results (atom {}))

(defn- mock-query-items [query-params options]
  (reset! mock-query-items-results
          (and (= :eq (first (:lahetystila query-params)))
               (= :s (first (second (:lahetystila query-params))))
               (= (:viestintapalvelussa c/kasittelytilat)
                  (second (second (:lahetystila query-params))))
               (= "lahetysIndex" (:index options))
               (= 10 (:limit options)))))

(deftest test-do-query
  (testing "Varmista, että do-query kutsuu query-items oikein"
    (with-redefs [oph.heratepalvelu.db.dynamodb/query-items mock-query-items]
      (esh/do-query)
      (is (true? @mock-query-items-results)))))


;; TODO -handleEmailStatus
