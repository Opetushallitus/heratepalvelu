(ns oph.heratepalvelu.amis.AMISEmailResendHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.amis.AMISEmailResendHandler :as erh]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.test-util :as tu]))

(deftest test-resend-checker
  (testing "Varmista, että resend-checker tarkistaa herätteitä oikein"
    (let [good1 {:kyselylinkki "kysely.linkki/1234"
                 :sahkoposti "a@b.com"}
          good2 {:kyselylinkki "kysely.linkki/789"}
          bad1  {:sahkoposti "a@b.com"}
          bad2  {:kyselylinkki "kysely.linkki/2345"
                 :sahkoposti ""}]
      (is (nil? (erh/resend-checker good1)))
      (is (nil? (erh/resend-checker good2)))
      (is (some? (erh/resend-checker bad1)))
      (is (some? (erh/resend-checker bad2))))))

(defn- mock-query-items [query-params index-data]
  (when (and (= :eq (first (:kyselylinkki query-params)))
             (= :s (first (second (:kyselylinkki query-params))))
             (= "kysely.linkki/12"
                (second (second (:kyselylinkki query-params))))
             (= "resendIndex" (:index index-data)))
    [{:kyselylinkki "kysely.linkki/12"}]))

(deftest test-get-one-item-by-kyselylinkki
  (testing
    "Varmista, että get-one-item-by-kyselylinkki kutsuu query-items oikein"
    (with-redefs [oph.heratepalvelu.db.dynamodb/query-items mock-query-items]
      (is (= {:kyselylinkki "kysely.linkki/12"}
             (erh/get-one-item-by-kyselylinkki "kysely.linkki/12"))))))

(def update-results (atom {}))

(defn- mock-update-item [query-params options]
  (when (and (= :s (first (:toimija_oppija query-params)))
             (= :s (first (:tyyppi_kausi query-params)))
             (:update-expr options)
             (:expr-attr-names options)
             (:expr-attr-vals options))
    (reset! update-results
            {:toimija-oppija (second (:toimija_oppija query-params))
             :tyyppi-kausi (second (:tyyppi_kausi query-params))
             :sahkoposti (second (get (:expr-attr-vals options) ":sposti"))
             :tila (second (get (:expr-attr-vals options) ":lahetystila"))})))

(deftest test-update-email-to-resend
  (testing "Varmista, että update-email-to-resend kutsuu update-item oikein"
    (with-redefs [oph.heratepalvelu.db.dynamodb/update-item mock-update-item]
      (erh/update-email-to-resend "toimija-oppija"
                                  "tyyppi-kausi"
                                  "sähköposti"
                                  "kysely.linkki/123")
      (is (= @update-results {:toimija-oppija "toimija-oppija"
                              :tyyppi-kausi "tyyppi-kausi"
                              :sahkoposti "sähköposti"
                              :tila (:ei-lahetetty c/kasittelytilat)})))))

(def update-email-to-resend-result (atom {}))

(defn- mock-update-email-to-resend [toimija-oppija
                                    tyyppi-kausi
                                    sahkoposti
                                    kyselylinkki]
  (reset! update-email-to-resend-result {:toimija-oppija toimija-oppija
                                         :tyyppi-kausi tyyppi-kausi
                                         :sahkoposti sahkoposti
                                         :kyselylinkki kyselylinkki}))

(defn- mock-get-one-item-by-kyselylinkki [kyselylinkki]
  {:kyselylinkki kyselylinkki
   :toimija_oppija "toimija-oppija"
   :tyyppi_kausi "tyyppi-kausi"
   :sahkoposti "a@b.com"})

(defn- mock-return-nothing [kyselylinkki] nil)

(deftest test-handleEmailResend
  (testing "Varmista, että -handleEmailResend toimii oikein"
    (with-redefs
      [oph.heratepalvelu.amis.AMISEmailResendHandler/update-email-to-resend
       mock-update-email-to-resend
       oph.heratepalvelu.amis.AMISEmailResendHandler/get-one-item-by-kyselylinkki
       mock-get-one-item-by-kyselylinkki]
      (let [context (tu/mock-handler-context)
            bad-event (tu/mock-sqs-event {:asdf "AMISEmailResendHandler xyz"})
            good-event (tu/mock-sqs-event {:kyselylinkki "https://linkki.fi/1"
                                           :sahkoposti "x@y.com"})
            good-event-no-email (tu/mock-sqs-event
                                  {:kyselylinkki "https://linkki.fi/1"})]
        (erh/-handleEmailResend {} bad-event context)
        (is (true? (tu/did-log? ":herate {:asdf AMISEmailResendHandler xyz},"
                                "ERROR")))
        (erh/-handleEmailResend {} good-event context)
        (is (= @update-email-to-resend-result
               {:toimija-oppija "toimija-oppija"
                :tyyppi-kausi "tyyppi-kausi"
                :sahkoposti "x@y.com"
                :kyselylinkki "https://linkki.fi/1"}))
        (is (nil?
              (tu/did-log?
                "Ei sähköpostia herätteessä  {:kyselylinkki https://linkki.fi/1"
                "WARN")))
        (erh/-handleEmailResend {} good-event-no-email context)
        (is (= @update-email-to-resend-result
               {:toimija-oppija "toimija-oppija"
                :tyyppi-kausi "tyyppi-kausi"
                :sahkoposti "a@b.com"
                :kyselylinkki "https://linkki.fi/1"}))
        (is (true?
              (tu/did-log?
                "Ei sähköpostia herätteessä  {:kyselylinkki https://linkki.fi/1"
                "WARN")))
        (with-redefs
          [oph.heratepalvelu.amis.AMISEmailResendHandler/get-one-item-by-kyselylinkki
           mock-return-nothing]
          (erh/-handleEmailResend {} good-event context)
          (is (true? (tu/did-log? "Ei kyselylinkkiä  https://linkki.fi/1"
                                  "ERROR"))))))))
