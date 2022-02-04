(ns oph.heratepalvelu.integration-tests.amis.AMISherateEmailHandler-i-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.amis.AMISherateEmailHandler :as heh]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.integration-tests.mock-cas-client :as mcc]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.integration-tests.mock-http-client :as mhc]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(def mock-env {:herate-table "herate-table-name"
               :viestintapalvelu-url "viestintapalvelu-example.com"
               :ehoks-url "ehoks-example.com/"
               :arvo-url "arvo-example.com/"
               :arvo-user "arvo-user"})

(def starting-table-contents [{:toimija_oppija [:s "abc/123"]
                               :tyyppi_kausi [:s "aloittaneet/2021-2022"]
                               :muistutukset [:n 2]
                               :kyselylinkki [:s "kysely.linkki/123"]
                               :sahkoposti [:s "sahko.posti@esimerkki.fi"]
                               :lahetystila [:s (:ei-lahetetty
                                                  c/kasittelytilat)]
                               :suorituskieli [:s "fi"]
                               :kyselytyyppi [:s "aloittaneet"]
                               :alkupvm [:s "2022-01-01"]}
                              {:toimija_oppija [:s "lkj/245"]
                               :tyyppi_kausi [:s "paattyneet/2022-2023"]
                               :kyselylinkki [:s "kysely.linkki/245"]
                               :sahkoposti [:s "asdf@esimerkki.fi"]
                               :lahetystila [:s (:ei-lahetetty
                                                  c/kasittelytilat)]
                               :suorituskieli [:s "fi"]
                               :kyselytyyppi [:s "tutkinnon_suorittaneet"]
                               :alkupvm [:s "2022-01-05"]}
                              {:toimija_oppija [:s "test/1"]
                               :tyyppi_kausi [:s "asdf"]
                               :lahetystila [:s (:success c/kasittelytilat)]
                               :alkupvm [:s "2022-06-06"]}
                              {:toimija_oppija [:s "test/2"]
                               :tyyppi_kausi [:s "asdf"]
                               :lahetystila [:s (:success c/kasittelytilat)]
                               :alkupvm [:s "2022-06-04"]}])

(defn- setup-test []
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mhc/bind-get-url (str (:arvo-url mock-env) "vastauslinkki/v1/status/123")
                    {:body {:voimassa_loppupvm "2022-02-28"}})
  (mhc/bind-get-url (str (:arvo-url mock-env) "vastauslinkki/v1/status/245")
                    {:body {:voimassa_loppupvm "2022-02-01"}})
  (mcc/clear-results)
  (mcc/clear-url-bindings)
  (mcc/bind-url :post
                (:viestintapalvelu-url mock-env)
                {:recipient [{:email "sahko.posti@esimerkki.fi"}]
                 :email {:callingProcess "heratepalvelu"
                         :from "no-reply@opintopolku.fi"
                         :sender "Opetushallitus – Utbildningsstyrelsen – EDUFI"
                         :subject (str "Palautetta oppilaitokselle - "
                                       "Respons till läroanstalten - "
                                       "Feedback to educational institution")
                         :isHtml true
                         :body (vp/amispalaute-html
                                 {:suorituskieli "fi"
                                  :kyselylinkki "kysely.linkki/123"
                                  :kyselytyyppi "aloittaneet"})}}
                {:body {:id 123}})
  (mdb/clear-mock-db)
  (mdb/create-table (:herate-table mock-env) {:primary-key :toimija_oppija
                                              :sort-key :tyyppi_kausi})
  (mdb/add-index-key-fields (:herate-table mock-env)
                            "lahetysIndex"
                            {:primary-key :lahetystila
                             :sort-key :alkupvm})
  (mdb/set-table-contents (:herate-table mock-env) starting-table-contents))

(defn- teardown-test []
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mcc/clear-results)
  (mcc/clear-url-bindings)
  (mdb/clear-mock-db))

(def expected-table-contents #{{:toimija_oppija [:s "abc/123"]
                                :tyyppi_kausi [:s "aloittaneet/2021-2022"]
                                :muistutukset [:n 0]
                                :kyselylinkki [:s "kysely.linkki/123"]
                                :sahkoposti [:s "sahko.posti@esimerkki.fi"]
                                :lahetystila [:s (:viestintapalvelussa
                                                   c/kasittelytilat)]
                                :lahetyspvm [:s "2022-02-02"]
                                :suorituskieli [:s "fi"]
                                :kyselytyyppi [:s "aloittaneet"]
                                :alkupvm [:s "2022-01-01"]
                                :viestintapalvelu-id [:n 123]}
                               {:toimija_oppija [:s "lkj/245"]
                                :tyyppi_kausi [:s "paattyneet/2022-2023"]
                                :kyselylinkki [:s "kysely.linkki/245"]
                                :sahkoposti [:s "asdf@esimerkki.fi"]
                                :lahetystila [:s (:vastausaika-loppunut
                                                   c/kasittelytilat)]
                                :lahetyspvm [:s "2022-02-02"]
                                :suorituskieli [:s "fi"]
                                :kyselytyyppi [:s "tutkinnon_suorittaneet"]
                                :alkupvm [:s "2022-01-05"]}
                               {:toimija_oppija [:s "test/1"]
                                :tyyppi_kausi [:s "asdf"]
                                :lahetystila [:s (:success c/kasittelytilat)]
                                :alkupvm [:s "2022-06-06"]}
                               {:toimija_oppija [:s "test/2"]
                                :tyyppi_kausi [:s "asdf"]
                                :lahetystila [:s (:success c/kasittelytilat)]
                                :alkupvm [:s "2022-06-04"]}})

(def expected-http-results
  [{:method :get
    :url "arvo-example.com/vastauslinkki/v1/status/123"
    :options {:basic-auth ["arvo-user" "arvo-pwd"]
              :as :json}}
   {:method :patch
    :url "ehoks-example.com/hoks/kyselylinkki"
    :options {:headers {:ticket (str "service-ticket/ehoks-virkailija-backend"
                                     "/cas-security-check")}
              :content-type "application/json"
              :body (str "{\"kyselylinkki\":\"kysely.linkki/123\","
                         "\"lahetyspvm\":\"2022-02-02\","
                         "\"sahkoposti\":\"sahko.posti@esimerkki.fi\","
                         "\"lahetystila\":\"viestintapalvelussa\"}")
              :as :json}}
   {:method :get
    :url "arvo-example.com/vastauslinkki/v1/status/245"
    :options {:basic-auth ["arvo-user" "arvo-pwd"]
              :as :json}}])

(def expected-cas-client-results
  [{:method :post
    :url (:viestintapalvelu-url mock-env)
    :body {:recipient [{:email "sahko.posti@esimerkki.fi"}]
           :email {:callingProcess "heratepalvelu"
                   :from "no-reply@opintopolku.fi"
                   :sender "Opetushallitus – Utbildningsstyrelsen – EDUFI"
                   :subject (str "Palautetta oppilaitokselle - "
                                 "Respons till läroanstalten - "
                                 "Feedback to educational institution")
                   :isHtml true
                   :body (vp/amispalaute-html {:suorituskieli "fi"
                                               :kyselylinkki "kysely.linkki/123"
                                               :kyselytyyppi "aloittaneet"})}}
    :options {:as :json}}
   {:type :get-service-ticket
    :service "/ehoks-virkailija-backend"
    :suffix "cas-security-check"}])

(deftest test-AMISherateEmailHandler-integration
  (testing "AMISherateEmailHandler integraatiotesti"
    (with-redefs [environ.core/env mock-env
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 2 2))
                  oph.heratepalvelu.db.dynamodb/query-items mdb/query-items
                  oph.heratepalvelu.db.dynamodb/update-item mdb/update-item
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.cas-client/cas-authenticated-post
                  mcc/mock-cas-authenticated-post
                  oph.heratepalvelu.external.cas-client/get-service-ticket
                  mcc/mock-get-service-ticket
                  oph.heratepalvelu.external.http-client/get mhc/mock-get
                  oph.heratepalvelu.external.http-client/patch mhc/mock-patch]
      (setup-test)
      (heh/-handleSendAMISEmails {}
                                 (tu/mock-handler-event :scheduledherate)
                                 (tu/mock-handler-context))
      (is (= (mdb/get-table-values (:herate-table mock-env))
             expected-table-contents))
      (is (= (mhc/get-results) expected-http-results))
      (is (= (mcc/get-results) expected-cas-client-results))
      (teardown-test))))
