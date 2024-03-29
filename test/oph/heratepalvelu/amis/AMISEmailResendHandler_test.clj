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
      (is (nil? (erh/resend-schema-errors good1)))
      (is (nil? (erh/resend-schema-errors good2)))
      (is (some? (erh/resend-schema-errors bad1)))
      (is (some? (erh/resend-schema-errors bad2))))))

(def results (atom {}))

(defn- mock-update-herate [herate updates]
  (reset! results {:herate herate :updates updates}))

(defn- mock-get-item-by-kyselylinkki [kyselylinkki]
  {:kyselylinkki kyselylinkki
   :toimija_oppija "toimija-oppija"
   :tyyppi_kausi "tyyppi-kausi"
   :sahkoposti "a@b.com"})

(defn- mock-return-nothing [_] nil)

(deftest test-handleEmailResend
  (testing "Varmista, että -handleEmailResend toimii oikein"
    (with-redefs [oph.heratepalvelu.amis.AMISCommon/get-herate-by-kyselylinkki!
                  mock-get-item-by-kyselylinkki
                  oph.heratepalvelu.amis.AMISCommon/update-herate
                  mock-update-herate]
      (let [context (tu/mock-handler-context)
            bad-event (tu/mock-sqs-event {:asdf "AMISEmailResendHandler xyz"})
            good-event (tu/mock-sqs-event {:kyselylinkki "https://linkki.fi/1"
                                           :sahkoposti "x@y.com"})
            good-event-no-email (tu/mock-sqs-event
                                  {:kyselylinkki "https://linkki.fi/1"})]
        (erh/-handleEmailResend {} bad-event context)
        (is (true? (tu/did-log? "Epämuodostunut heräte" "ERROR")))
        (erh/-handleEmailResend {} good-event context)
        (is (= @results {:herate {:kyselylinkki "https://linkki.fi/1"
                                  :toimija_oppija "toimija-oppija"
                                  :tyyppi_kausi "tyyppi-kausi"
                                  :sahkoposti "a@b.com"}
                         :updates {:sahkoposti [:s "x@y.com"]
                                   :lahetystila
                                   [:s (:ei-lahetetty c/kasittelytilat)]}}))
        (is (nil?
              (tu/did-log?
                "Ei sähköpostia herätteessä {:kyselylinkki https://linkki.fi/1"
                "WARN")))
        (erh/-handleEmailResend {} good-event-no-email context)
        (is (= @results
               {:herate {:kyselylinkki "https://linkki.fi/1"
                         :toimija_oppija "toimija-oppija"
                         :tyyppi_kausi "tyyppi-kausi"
                         :sahkoposti "a@b.com"}
                :updates {:sahkoposti [:s "a@b.com"]
                          :lahetystila [:s (:ei-lahetetty c/kasittelytilat)]}}))
        (is (true? (tu/did-log? "Ei sähköpostia, käytetään" "WARN")))
        (with-redefs
          [oph.heratepalvelu.amis.AMISCommon/get-herate-by-kyselylinkki!
           mock-return-nothing]
          (erh/-handleEmailResend {} good-event context)
          (is (true? (tu/did-log? "Ei löytynyt herätettä" "ERROR"))))))))
