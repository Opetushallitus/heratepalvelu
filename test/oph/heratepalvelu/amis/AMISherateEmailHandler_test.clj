(ns oph.heratepalvelu.amis.AMISherateEmailHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.amis.AMISherateEmailHandler :as heh]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]))

(def mock-send-lahetys-data-to-ehoks-result (atom {}))

(defn- mock-send-lahetys-data-to-ehoks [tomija-oppija tyyppi-kausi data]
  (reset! mock-send-lahetys-data-to-ehoks-result
          {:toimija-oppija toimija-oppija
           :tyyppi-kausi tyyppi-kausi
           :kyselylinkki (:kyselylinkki data)
           :lahetyspvm (:lahetyspvm data)
           :sahkoposti (:sahkoposti data)
           :lahetystila (:lahetystila data)}))









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
                      :sender "Opetushallitus – Utbildningsstyrelsen"}]
        (is (= expected (heh/send-feedback-email email)))))))
