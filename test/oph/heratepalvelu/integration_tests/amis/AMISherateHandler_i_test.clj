(ns oph.heratepalvelu.integration-tests.amis.AMISherateHandler-i-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.amis.AMISherateHandler :as hh]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.integration-tests.mock-cas-client :as mcc]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.integration-tests.mock-http-client :as mhc]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(def mock-env {:herate-table "herate-table-name"
               :arvo-url "https://oph-arvo.com/"
               :arvo-user "arvo-user"
               :ehoks-url "https://oph-ehoks.com/"
               :koski-url "https://oph-koski.com"
               :koski-user "koski-user"
               :organisaatio-url "https://oph-organisaatio.com/"})

(def starting-table-contents [{:toimija_oppija [:s "abc/123"]
                               :tyyppi_kausi [:s "aloittaneet/2022-2023"]
                               :muistutukset [:n 2]
                               :kyselylinkki [:s "kysely.linkki/123"]
                               :sahkoposti [:s "sahko.posti@esimerkki.fi"]
                               :viestintapalvelu-id [:n 123]}])

(def herate {:ehoks-id 456
             :kyselytyyppi "aloittaneet"
             :opiskeluoikeus-oid "123.5.9876"
             :oppija-oid "3.4.5"
             :sahkoposti "foo@bar.com"
             :puhelinnumero "1234567"
             :alkupvm "2022-07-05"})

(defn- setup-test []
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mhc/bind-url :get
                (str (:koski-url mock-env) "/opiskeluoikeus/123.5.9876")
                {:basic-auth [(:koski-user mock-env) "koski-pwd"] :as :json}
                {:body {:oid "123.5.9876"
                        :koulutustoimija {:oid "test-koulutustoimija-oid"}
                        :oppilaitos {:oid "test-oppilaitos-oid"}
                        :lisätiedot {:maksuttomuus [{:alku "2020-01-01"
                                                     :loppu "2022-12-31"
                                                     :maksuton true}]
                                     :erityinenTuki [{:alku "2020-01-01"
                                                      :loppu "2022-12-31"}]}
                        :suoritukset
                        [{:tyyppi {:koodiarvo "ammatillinentutkinto"}
                          :suorituskieli {:koodiarvo "fi"}
                          :toimipiste {:oid "test-toimipiste-oid"}}]}})
  (mhc/bind-url :get
                (str (:organisaatio-url mock-env) "test-toimipiste-oid")
                {:as :json}
                {:body {:tyypit ["organisaatiotyyppi_03"]}})
  (mhc/bind-url :post
                (str (:arvo-url mock-env) "vastauslinkki/v1")
                {:content-type "application/json"
                 :basic-auth [(:arvo-user mock-env) "arvo-pwd"]
                 :as :json
                 :body (str
                         "{\"vastaamisajan_alkupvm\":\"2022-02-02\","
                         "\"osaamisala\":null,\"heratepvm\":\"2022-01-05\","
                         "\"koulutustoimija_oid\":\"test-koulutustoimija-oid\","
                         "\"tutkinnon_suorituskieli\":\"fi\","
                         "\"toimipiste_oid\":\"test-toimipiste-oid\","
                         "\"oppilaitos_oid\":\"test-oppilaitos-oid\","
                         "\"hankintakoulutuksen_toteuttaja\":null,"
                         "\"kyselyn_tyyppi\":\"aloittaneet\","
                         "\"tutkintotunnus\":null,\"request_id\":\"test-uuid\","
                         "\"vastaamisajan_loppupvm\":\"2022-03-03\"}")}
                {:body {:kysely_linkki "kysely.linkki/ABCDE"}})
  (mcc/clear-results)
  (mdb/clear-mock-db)
  (mdb/create-table (:herate-table mock-env) {:primary-key :toimija_oppija
                                              :sort-key :tyyppi_kausi})
  (mdb/set-table-contents (:herate-table mock-env) starting-table-contents))

(defn- teardown-test []
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mcc/clear-results)
  (mdb/clear-mock-db))

(def expected-table-contents
  #{{:toimija_oppija [:s "abc/123"]
     :tyyppi_kausi [:s "aloittaneet/2022-2023"]
     :muistutukset [:n 2]
     :kyselylinkki [:s "kysely.linkki/123"]
     :sahkoposti [:s "sahko.posti@esimerkki.fi"]
     :viestintapalvelu-id [:n 123]}
    {:toimija_oppija [:s "test-koulutustoimija-oid/3.4.5"]
     :tyyppi_kausi [:s "aloittaneet/2022-2023"]
     :kyselytyyppi [:s "aloittaneet"]
     :request-id [:s "test-uuid"]
     :voimassa-loppupvm [:s "2022-08-03"]
     :hankintakoulutuksen-toteuttaja [:s ""]
     :suorituskieli [:s "fi"]
     :sahkoposti [:s "foo@bar.com"]
     :puhelinnumero [:s "1234567"]
     :osaamisala [:s ""]
     :heratepvm [:s "2022-07-05"]
     :lahetystila [:s (:ei-lahetetty c/kasittelytilat)]
     :sms-lahetystila [:s (:ei-laheteta c/kasittelytilat)]
     :tallennuspvm [:s "2022-02-02"]
     :oppilaitos [:s "test-oppilaitos-oid"]
     :toimipiste-oid [:s "test-toimipiste-oid"]
     :viestintapalvelu-id [:n "-1"]
     :opiskeluoikeus-oid [:s "123.5.9876"]
     :alkupvm [:s "2022-07-05"]
     :koulutustoimija [:s "test-koulutustoimija-oid"]
     :tutkintotunnus [:s ""]
     :oppija-oid [:s "3.4.5"]
     :ehoks-id [:n "456"]
     :rahoituskausi [:s "2022-2023"]
     :rahoitusryhma [:s "01"]
     :herate-source [:s (:ehoks c/herate-sources)]}})

(def expected-http-results
  [{:method :get
    :url "https://oph-koski.com/opiskeluoikeus/123.5.9876"
    :options {:basic-auth ["koski-user" "koski-pwd"]
              :as :json}}
   {:method :get
    :url "https://oph-organisaatio.com/test-toimipiste-oid"
    :options {:as :json}}
   {:method :get
    :url "https://oph-ehoks.com/hoks/456/hankintakoulutukset"
    :options
    {:headers
     {:ticket "service-ticket/ehoks-virkailija-backend/cas-security-check"}
     :as :json}}
   {:method :patch
    :url (str (:ehoks-url mock-env)
              "heratepalvelu/hoksit/456/aloitusherate-kasitelty")
    :options
    {:headers
     {:ticket "service-ticket/ehoks-virkailija-backend/cas-security-check"}
     :content-type "application/json"
     :as :json}}])

(def expected-cas-client-results [{:type :get-service-ticket
                                   :service "/ehoks-virkailija-backend"
                                   :suffix "cas-security-check"}
                                  {:type :get-service-ticket
                                   :service "/ehoks-virkailija-backend"
                                   :suffix "cas-security-check"}])

(deftest test-AMISherateHandler-integration
  (testing "AMISherateHandler integraatiotesti"
    (with-redefs [environ.core/env mock-env
                  oph.heratepalvelu.common/generate-uuid (fn [] "test-uuid")
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 2 2))
                  oph.heratepalvelu.db.dynamodb/get-item mdb/get-item
                  oph.heratepalvelu.db.dynamodb/put-item mdb/put-item
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.cas-client/get-service-ticket
                  mcc/mock-get-service-ticket
                  oph.heratepalvelu.external.koski/pwd (delay "koski-pwd")
                  oph.heratepalvelu.external.http-client/get mhc/mock-get
                  oph.heratepalvelu.external.http-client/patch mhc/mock-patch
                  oph.heratepalvelu.external.http-client/post mhc/mock-post]
      (setup-test)
      (hh/-handleAMISherate {}
                            (tu/mock-sqs-event herate)
                            (tu/mock-handler-context))
      (is (= (mdb/get-table-values (:herate-table mock-env))
             expected-table-contents))
      (is (= (mhc/get-results) expected-http-results))
      (is (= (mcc/get-results) expected-cas-client-results))
      (teardown-test))))
