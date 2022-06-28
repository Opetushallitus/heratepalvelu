(ns oph.heratepalvelu.integration-tests.amis.UpdatedOpiskeluoikeusHandler-i-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.amis.UpdatedOpiskeluoikeusHandler :as uoh]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.integration-tests.mock-cas-client :as mcc]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.integration-tests.mock-http-client :as mhc]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(def mock-env {:orgwhitelist-table "orgwhitelist-table-name"
               :metadata-table "metadata-table-name"
               :herate-table "herate-table-name"
               :arvo-url "https://oph-arvo.com/"
               :arvo-user "arvo-user"
               :ehoks-url "https://oph-ehoks.com/"
               :koski-url "https://oph-koski.com"
               :koski-user "koski-user"
               :organisaatio-url "https://oph-organisaatio.com"})

(def starting-table-contents [{:key [:s "opiskeluoikeus-last-checked"]
                               :value [:s "2022-02-02T00:00:00.000Z"]}
                              {:key [:s "opiskeluoikeus-last-page"]
                               :value [:s "0"]}])

(def starting-ow-table [{:organisaatio-oid [:s "test-ktid"]
                         :kayttoonottopvm [:s "2022-01-06"]}
                        {:organisaatio-oid [:s "test-ktid2"]
                         :kayttoonottopvm [:s "2022-01-27"]}])

(defn- setup-test []
  (mcc/clear-results)
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mhc/bind-url :get
                (str (:koski-url mock-env) "/oppija/")
                {:query-params {"opiskeluoikeudenTyyppi" "ammatillinenkoulutus"
                                "muuttunutJälkeen" "2022-02-02T00:00:00.000Z"
                                "pageSize" 100
                                "pageNumber" 0}
                 :basic-auth [(:koski-user mock-env) "koski-pwd"]
                 :as :json-strict}
                {:body [{:opiskeluoikeudet
                         [{:oid "12345"
                           :aikaleima "2022-01-05"
                           :koulutustoimija {:oid "test-ktid"}
                           :suoritukset [{:vahvistus {:päivä "2022-01-05"}
                                          :tyyppi {:koodiarvo
                                                   "ammatillinentutkinto"}
                                          :suorituskieli {:koodiarvo "fi"}}]
                           :sisältyyOpiskeluoikeuteen nil
                           :tila {:opiskeluoikeusjaksot
                                  [{:alku "2022-01-05"
                                    :tila {:koodiarvo "lasna"}}]}}]}

                        {:opiskeluoikeudet
                         [{:oid "99999"
                           :aikaleima "2022-01-07"
                           :koulutustoimija {:oid "test-ktid2"}
                           :suoritukset [{:vahvistus {:päivä "2022-01-07"}
                                          :tyyppi {:koodiarvo
                                                   "ammatillinentutkinto"}
                                          :suorituskieli {:koodiarvo "fi"}}]
                           :sisältyyOpiskeluoikeuteen nil
                           :tila {:opiskeluoikeusjaksot
                                  [{:alku "2022-01-07"
                                    :tila {:koodiarvo "lasna"}}]}}]}]})
  (mhc/bind-url :get
                (str (:ehoks-url mock-env) "hoks/opiskeluoikeus/12345")
                {:headers {:ticket (str "service-ticket"
                                        "/ehoks-virkailija-backend"
                                        "/cas-security-check")}
                 :as :json}
                {:body {:data {:id 123
                               :oppija-oid "1.2.3"
                               :opiskeluoikeus-oid "12345"
                               :osaamisen-hankkimisen-tarve true
                               :sahkoposti "test@example.com"}}})
  (mhc/bind-url :get
                (str (:ehoks-url mock-env) "hoks/opiskeluoikeus/99999")
                {:headers {:ticket (str "service-ticket"
                                        "/ehoks-virkailija-backend"
                                        "/cas-security-check")}
                 :as :json}
                {:body {:data {:id 999
                               :oppija-oid "9.9.9"
                               :opiskeluoikeus-oid "99999"
                               :osaamisen-hankkimisen-tarve false
                               :sahkoposti "foo@example.com"}}})
  (mhc/bind-url :post
                (str (:arvo-url mock-env) "vastauslinkki/v1")
                {:content-type "application/json"
                 :basic-auth [(:arvo-user mock-env) "arvo-pwd"]
                 :as :json
                 :body (str "{\"vastaamisajan_alkupvm\":\"2022-02-02\","
                            "\"osaamisala\":null,\"heratepvm\":\"2022-01-05\","
                            "\"koulutustoimija_oid\":\"test-ktid\","
                            "\"tutkinnon_suorituskieli\":\"fi\","
                            "\"toimipiste_oid\":null,\"oppilaitos_oid\":null,"
                            "\"hankintakoulutuksen_toteuttaja\":null,"
                            "\"kyselyn_tyyppi\":\"tutkinnon_suorittaneet\","
                            "\"tutkintotunnus\":null,"
                            "\"request_id\":\"test-uuid\","
                            "\"vastaamisajan_loppupvm\":\"2022-03-03\"}")}
                {:body {:kysely_linkki "kysely.linkki/12345"}})
  (mdb/clear-mock-db)
  (mdb/create-table (:metadata-table mock-env) {:primary-key :key})
  (mdb/set-table-contents (:metadata-table mock-env) starting-table-contents)
  (mdb/create-table (:orgwhitelist-table mock-env)
                    {:primary-key :organisaatio-oid})
  (mdb/set-table-contents (:orgwhitelist-table mock-env) starting-ow-table)
  (mdb/create-table (:herate-table mock-env) {:primary-key :toimija_oppija
                                              :sort-key :tyyppi_kausi})
  (mdb/set-table-contents (:herate-table mock-env) []))

(defn- teardown-test []
  (mcc/clear-results)
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mdb/clear-mock-db))

(def expected-table-contents #{{:key [:s "opiskeluoikeus-last-checked"]
                                :value [:s "2022-02-02T00:00:00.000Z"]}
                               {:key [:s "opiskeluoikeus-last-page"]
                                :value [:s "1"]}})

(def expected-ow-table (into #{} starting-ow-table))

(def expected-herate-table
  #{{:toimija_oppija [:s "test-ktid/1.2.3"]
     :tyyppi_kausi [:s "tutkinnon_suorittaneet/2021-2022"]
     :kyselytyyppi [:s "tutkinnon_suorittaneet"]
     :request-id [:s "test-uuid"]
     :voimassa-loppupvm [:s "2022-03-03"]
     :hankintakoulutuksen-toteuttaja [:s ""]
     :suorituskieli [:s "fi"]
     :sahkoposti [:s "test@example.com"]
     :osaamisala [:s ""]
     :heratepvm [:s "2022-01-05"]
     :lahetystila [:s (:ei-lahetetty c/kasittelytilat)]
     :tallennuspvm [:s "2022-02-02"]
     :oppilaitos [:s nil]
     :toimipiste-oid [:s ""]
     :viestintapalvelu-id [:n "-1"]
     :opiskeluoikeus-oid [:s "12345"]
     :alkupvm [:s "2022-02-02"]
     :koulutustoimija [:s "test-ktid"]
     :tutkintotunnus [:s ""]
     :oppija-oid [:s "1.2.3"]
     :ehoks-id [:n "123"]
     :rahoituskausi [:s "2021-2022"]
     :herate-source [:s (:koski c/herate-sources)]}})

(def expected-http-results
  [{:method :get
    :url (str (:koski-url mock-env) "/oppija/")
    :options {:query-params {"opiskeluoikeudenTyyppi" "ammatillinenkoulutus"
                             "muuttunutJälkeen" "2022-02-02T00:00:00.000Z"
                             "pageSize" 100
                             "pageNumber" 0}
              :basic-auth [(:koski-user mock-env) "koski-pwd"]
              :as :json-strict}}
   {:method :get
    :url (str (:ehoks-url mock-env) "hoks/opiskeluoikeus/12345")
    :options
    {:headers
     {:ticket "service-ticket/ehoks-virkailija-backend/cas-security-check"}
     :as :json}}
   {:method :get
    :url (:organisaatio-url mock-env)
    :options {:as :json}}
   {:method :get
    :url (str (:ehoks-url mock-env) "hoks/123/hankintakoulutukset")
    :options
    {:headers
     {:ticket "service-ticket/ehoks-virkailija-backend/cas-security-check"}
     :as :json}}
   {:method :patch
    :url (str (:ehoks-url mock-env)
              "heratepalvelu/hoksit/123/paattoherate-kasitelty")
    :options
    {:headers
     {:ticket "service-ticket/ehoks-virkailija-backend/cas-security-check"}
     :content-type "application/json"
     :as :json}}
   {:method :get
    :url (str (:ehoks-url mock-env) "hoks/opiskeluoikeus/99999")
    :options
    {:headers
     {:ticket "service-ticket/ehoks-virkailija-backend/cas-security-check"}
     :as :json}}])

(def expected-cas-client-results [{:type :get-service-ticket
                                   :service "/ehoks-virkailija-backend"
                                   :suffix "cas-security-check"}
                                  {:type :get-service-ticket
                                   :service "/ehoks-virkailija-backend"
                                   :suffix "cas-security-check"}
                                  {:type :get-service-ticket
                                   :service "/ehoks-virkailija-backend"
                                   :suffix "cas-security-check"}
                                  {:type :get-service-ticket
                                   :service "/ehoks-virkailija-backend"
                                   :suffix "cas-security-check"}])

(deftest test-UpdatedOpiskeluoikeusHandler-integration
  (testing "UpdatedOpiskeluoikeusHandler integraatiotesti"
    (with-redefs [environ.core/env mock-env
                  oph.heratepalvelu.common/generate-uuid (fn [] "test-uuid")
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 2 2))
                  oph.heratepalvelu.db.dynamodb/delete-item mdb/delete-item
                  oph.heratepalvelu.db.dynamodb/get-item mdb/get-item
                  oph.heratepalvelu.db.dynamodb/put-item mdb/put-item
                  oph.heratepalvelu.db.dynamodb/update-item mdb/update-item
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.cas-client/get-service-ticket
                  mcc/mock-get-service-ticket
                  oph.heratepalvelu.external.http-client/get mhc/mock-get
                  oph.heratepalvelu.external.http-client/patch mhc/mock-patch
                  oph.heratepalvelu.external.http-client/post mhc/mock-post
                  oph.heratepalvelu.external.koski/pwd (delay "koski-pwd")]
      (setup-test)
      (uoh/-handleUpdatedOpiskeluoikeus {}
                                        (tu/mock-handler-event :scheduledherate)
                                        (tu/mock-handler-context))
      (is (= (mdb/get-table-values (:metadata-table mock-env))
             expected-table-contents))
      (is (= (mdb/get-table-values (:orgwhitelist-table mock-env))
             expected-ow-table))
      (is (= (mdb/get-table-values (:herate-table mock-env))
             expected-herate-table))
      (is (= (mhc/get-results) expected-http-results))
      (is (= (mcc/get-results) expected-cas-client-results))
      (teardown-test))))
