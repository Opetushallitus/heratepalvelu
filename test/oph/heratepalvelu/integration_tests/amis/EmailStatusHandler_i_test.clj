(ns oph.heratepalvelu.integration-tests.amis.EmailStatusHandler-i-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.amis.EmailStatusHandler :as esh]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.integration-tests.mock-cas-client :as mcc]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.integration-tests.mock-http-client :as mhc]
            [oph.heratepalvelu.test-util :as tu]))

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
                               :lahetystila [:s (:viestintapalvelussa
                                                  c/kasittelytilat)]
                               :viestintapalvelu-id [:n 123]}
                              {:toimija_oppija [:s "lkj/245"]
                               :tyyppi_kausi [:s "paattyneet/2022-2023"]
                               :kyselylinkki [:s "kysely.linkki/245"]
                               :sahkoposti [:s "asdf@esimerkki.fi"]
                               :lahetystila [:s (:viestintapalvelussa
                                                  c/kasittelytilat)]
                               :viestintapalvelu-id [:n 245]}])

(defn- setup-test []
  (mhc/clear-results)
  (mcc/clear-results)
  (mcc/clear-url-bindings)
  (mcc/bind-url :post
                (str (:viestintapalvelu-url mock-env) "/status")
                123
                {:body {:numberOfSuccessfulSendings 1
                        :sendingEnded "2022-01-06T12:34:56"}})
  (mcc/bind-url :post
                (str (:viestintapalvelu-url mock-env) "/status")
                245
                {:body {:numberOfFailedSendings 1
                        :sendingEnded "2022-02-02T10:10:11"}})
  (mdb/clear-mock-db)
  (mdb/create-table (:herate-table mock-env) {:primary-key :toimija_oppija
                                              :sort-key :tyyppi_kausi})
  (mdb/set-table-contents (:herate-table mock-env) starting-table-contents))

(defn- teardown-test []
  (mhc/clear-results)
  (mcc/clear-results)
  (mcc/clear-url-bindings)
  (mdb/clear-mock-db))

(def expected-table-contents #{{:toimija_oppija [:s "abc/123"]
                                :tyyppi_kausi [:s "aloittaneet/2021-2022"]
                                :muistutukset [:n 2]
                                :kyselylinkki [:s "kysely.linkki/123"]
                                :sahkoposti [:s "sahko.posti@esimerkki.fi"]
                                :lahetystila [:s (:success c/kasittelytilat)]
                                :viestintapalvelu-id [:n 123]}
                               {:toimija_oppija [:s "lkj/245"]
                                :tyyppi_kausi [:s "paattyneet/2022-2023"]
                                :kyselylinkki [:s "kysely.linkki/245"]
                                :sahkoposti [:s "asdf@esimerkki.fi"]
                                :lahetystila [:s (:failed c/kasittelytilat)]
                                :viestintapalvelu-id [:n 245]}})

(def expected-http-results
  [{:method :patch
    :url "arvo-example.com/vastauslinkki/v1/123/metatiedot"
    :options {:basic-auth ["arvo-user" "arvo-pwd"]
              :content-type "application/json"
              :body "{\"tila\":\"lahetetty\"}"
              :as :json}}
   {:method :patch
    :url "arvo-example.com/vastauslinkki/v1/245/metatiedot"
    :options {:basic-auth ["arvo-user" "arvo-pwd"]
              :content-type "application/json"
              :body "{\"tila\":\"lahetys_epaonnistunut\"}"
              :as :json}}
   {:method :patch
    :url "ehoks-example.com/hoks/kyselylinkki"
    :options {:headers {:ticket (str "service-ticket/ehoks-virkailija-backend"
                                     "/cas-security-check")}
              :content-type "application/json"
              :body (str "{\"kyselylinkki\":\"kysely.linkki/245\","
                         "\"lahetyspvm\":\"2022-02-02\","
                         "\"sahkoposti\":\"asdf@esimerkki.fi\","
                         "\"lahetystila\":\"lahetys_epaonnistunut\"}")
              :as :json}}])

(def expected-cas-client-results [{:method :post
                                   :url "viestintapalvelu-example.com/status"
                                   :body 123
                                   :options {:as :json}}
                                  {:method :post
                                   :url "viestintapalvelu-example.com/status"
                                   :body 245
                                   :options {:as :json}}
                                  {:type :get-service-ticket
                                   :service "/ehoks-virkailija-backend"
                                   :suffix "cas-security-check"}])

(deftest test-EmailStatusHandler-integration
  (testing "EmailStatusHandlerin integraatiotesti"
    (with-redefs [environ.core/env mock-env
                  oph.heratepalvelu.db.dynamodb/get-item mdb/get-item
                  oph.heratepalvelu.db.dynamodb/query-items mdb/query-items
                  oph.heratepalvelu.db.dynamodb/update-item mdb/update-item
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.cas-client/cas-authenticated-post
                  mcc/mock-cas-authenticated-post
                  oph.heratepalvelu.external.cas-client/get-service-ticket
                  mcc/mock-get-service-ticket
                  oph.heratepalvelu.external.http-client/patch mhc/mock-patch]
      (setup-test)
      (esh/-handleEmailStatus {}
                              (tu/mock-handler-event :scheduledherate)
                              (tu/mock-handler-context))
      (is (= (mdb/get-table-values (:herate-table mock-env))
             expected-table-contents))
      (is (= (mhc/get-results) expected-http-results))
      (is (= (mcc/get-results) expected-cas-client-results))
      (teardown-test))))
