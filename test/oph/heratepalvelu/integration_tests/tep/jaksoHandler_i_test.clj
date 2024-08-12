(ns oph.heratepalvelu.integration-tests.tep.jaksoHandler-i-test
  (:require [clojure.test :refer :all]
            [clojure.data :refer [diff]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.integration-tests.mock-cas-client :as mcc]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.integration-tests.mock-http-client :as mhc]
            [oph.heratepalvelu.tep.jaksoHandler :as jh]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(def mock-env {:jaksotunnus-table "jaksotunnus-table-name"
               :nippu-table "nippu-table-name"
               :arvo-url "https://oph-arvo.com/"
               :arvo-user "arvo-user"
               :ehoks-url "https://oph-ehoks.com/"
               :koski-url "https://oph-koski.com"
               :koski-user "koski-user"
               :organisaatio-url "https://oph-organisaatio.com/"})

(def test-herate {:tyyppi "aloittaneet"
                  :alkupvm "2022-01-05"
                  :loppupvm "2022-02-02"
                  :hoks-id 123
                  :yksiloiva-tunniste "1234"
                  :opiskeluoikeus-oid "test-oo-oid"
                  :oppija-oid "test-oppija-oid"
                  :hankkimistapa-id 234
                  :hankkimistapa-tyyppi "oppisopimus"
                  :tutkinnonosa-id 1
                  :tutkinnonosa-koodi "test-tutkinnonosa"
                  :tutkinnonosa-nimi "Test Kurssi"
                  :tyopaikan-nimi "Testi Oy"
                  :tyopaikan-ytunnus "123456-7"
                  :tyopaikkaohjaaja-email "ohjaaja@esimerkki.fi"
                  :tyopaikkaohjaaja-nimi "Olli Ohjaaja"
                  :osa-aikaisuus 50
                  :oppisopimuksen-perusta "oppisopimuksenperusta_01"
                  :tyopaikkaohjaaja-puhelinnumero "+358 0401234567"
                  :keskeytymisajanjaksot [{:alku "2022-01-08"
                                           :loppu "2022-01-14"}]})

(def test-herate-externalfunding
  (assoc test-herate
         :oppija-oid "test-extfund-oppija-oid"
         :opiskeluoikeus-oid "test-oo-oid-externalfunding"
         :hankkimistapa-id 456
         :hoks-id 345
         :yksiloiva-tunniste "2345"))

(def test-herate-missing-osa-aikaisuus
  (assoc test-herate
         :osa-aikaisuus nil
         :loppupvm "2023-09-20"))

(def duplicate-herate {:tyyppi "aloittaneet"
                       :alkupvm "2022-01-01"
                       :loppupvm "2022-02-06"
                       :hoks-id 123
                       :yksiloiva-tunniste "1234"
                       :opiskeluoikeus-oid "test-oo-oid2"
                       :oppija-oid "test-oppija-oid2"
                       :hankkimistapa-id 567
                       :hankkimistapa-tyyppi "koulutussopimus"
                       :tutkinnonosa-id 2
                       :tutkinnonosa-koodi "test-tutkinnonosa2"
                       :tutkinnonosa-nimi "Test Kurssi 2"
                       :tyopaikan-nimi "Testi2 Oy"
                       :tyopaikan-ytunnus "123456-7"
                       :tyopaikkaohjaaja-email "ohjaaja@esimerkki.fi"
                       :tyopaikkaohjaaja-nimi "Olli Ohjaaja"})

(defn- opiskeluoikeus-response [rahoituskoodi1 rahoituskoodi2]
  {:oid "test-oo-oid"
   :koulutustoimija {:oid "koulutustoimija-oid"}
   :suoritukset [{:toimipiste {:oid "test-toimipiste"}
                  :tyyppi {:koodiarvo "ammatillinentutkinto"}
                  :koulutusmoduuli {:tunniste {:koodiarvo "testitutkinto"}}
                  :osaamisala [{:koodiarvo "test-osaamisala"}]
                  :tutkintonimike [{:koodiarvo "test-tutkintonimike"}]}]
   :tila {:opiskeluoikeusjaksot
          [{:alku "2022-01-05"
            :tila {:koodiarvo "lasna"}
            :opintojenRahoitus {:koodiarvo rahoituskoodi1}}
           {:alku "2021-10-08"
            :tila {:koodiarvo "lasna"}
            :opintojenRahoitus {:koodiarvo rahoituskoodi2}}]}
   :oppilaitos {:oid "testilaitos"}})

(defn- setup-test []
  (mcc/clear-results)
  (mcc/clear-url-bindings)
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mhc/bind-url :get
                (str (:koski-url mock-env) "/opiskeluoikeus/test-oo-oid")
                {:basic-auth [(:koski-user mock-env) "koski-pwd"] :as :json}
                {:body (opiskeluoikeus-response "1" "14")})
  (mhc/bind-url :get
                (str (:koski-url mock-env)
                     "/opiskeluoikeus/test-oo-oid-externalfunding")
                {:basic-auth [(:koski-user mock-env) "koski-pwd"] :as :json}
                {:body (opiskeluoikeus-response "14" "1")})
  (mhc/bind-url :get
                (str (:organisaatio-url mock-env) "test-toimipiste")
                {:as :json}
                {:body {:tyypit ["organisaatiotyyppi_03"]}})
  (mhc/bind-url :post
                (str (:arvo-url mock-env) "tyoelamapalaute/v1/vastaajatunnus")
                {:content-type "application/json"
                 :body (str "{\"osa_aikaisuus\":50,\"tyopaikka\":\"Testi Oy\","
                            "\"tyopaikka_normalisoitu\":\"testi_oy\","
                            "\"vastaamisajan_alkupvm\":\"2022-02-16\","
                            "\"tyonantaja\":\"123456-7\","
                            "\"oppisopimuksen_perusta\":\"01\","
                            "\"osaamisala\":[\"test-osaamisala\"],"
                            "\"koulutustoimija_oid\":\"koulutustoimija-oid\","
                            "\"paikallinen_tutkinnon_osa\":\"Test Kurssi\","
                            "\"rahoituskausi_pvm\":\"2022-02-02\","
                            "\"tyopaikkajakson_loppupvm\":\"2022-02-02\","
                            "\"toimipiste_oid\":\"test-toimipiste\","
                            "\"oppilaitos_oid\":\"testilaitos\","
                            "\"tyopaikkajakson_alkupvm\":\"2022-01-05\","
                            "\"tutkintotunnus\":\"testitutkinto\","
                            "\"sopimustyyppi\":\"oppisopimus\","
                            "\"tutkintonimike\":[\"test-tutkintonimike\"],"
                            "\"request_id\":\"test-uuid\","
                            "\"tutkinnon_osa\":\"test-tutkinnonosa\"}")
                 :basic-auth [(:arvo-user mock-env) "arvo-pwd"]
                 :as :json}
                {:body {:tunnus "ABCDEF"}})
  (mdb/clear-mock-db)
  (mdb/create-table (:jaksotunnus-table mock-env)
                    {:primary-key :hankkimistapa_id})
  (mdb/set-table-contents (:jaksotunnus-table mock-env) [])
  (mdb/add-index-key-fields (:jaksotunnus-table mock-env)
                            "uniikkiusIndex"
                            {:primary-key :tunnus})
  (mdb/create-table (:nippu-table mock-env)
                    {:primary-key :ohjaaja_ytunnus_kj_tutkinto
                     :sort-key :niputuspvm})
  (mdb/set-table-contents (:nippu-table mock-env) []))

(defn- teardown-test []
  (mcc/clear-results)
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mdb/clear-mock-db))

(def expected-jaksotunnus-table
  #{{:ohjaaja_ytunnus_kj_tutkinto
     [:s "Olli Ohjaaja/123456-7/koulutustoimija-oid/testitutkinto"]
     :niputuspvm [:s "2022-02-16"]
     :osa_aikaisuus [:n 50]
     :ohjaaja_nimi [:s "Olli Ohjaaja"]
     :tutkinnonosa_nimi [:s "Test Kurssi"]
     :opiskeluoikeus_oid [:s "test-oo-oid"]
     :hankkimistapa_tyyppi [:s "oppisopimus"]
     :hoks_id [:n 123]
     :yksiloiva_tunniste [:s "1234"]
     :oppisopimuksen_perusta [:s "01"]
     :tyopaikan_nimi [:s "Testi Oy"]
     :tyopaikan_ytunnus [:s "123456-7"]
     :jakso_loppupvm [:s "2022-02-02"]
     :ohjaaja_puhelinnumero [:s "+358 0401234567"]
     :osaamisala [:s "(\"test-osaamisala\")"]
     :tutkinnonosa_tyyppi [:s "aloittaneet"]
     :tutkinnonosa_koodi [:s "test-tutkinnonosa"]
     :tpk-niputuspvm [:s "ei_maaritelty"]
     :tallennuspvm [:s "2022-02-02"]
     :oppilaitos [:s "testilaitos"]
     :tunnus [:s "ABCDEF"]
     :tutkinnonosa_id [:n 1]
     :tyopaikan_normalisoitu_nimi [:s "testi_oy"]
     :toimipiste_oid [:s "test-toimipiste"]
     :tutkinto [:s "testitutkinto"]
     :alkupvm [:s "2022-02-16"]
     :koulutustoimija [:s "koulutustoimija-oid"]
     :jakso_alkupvm [:s "2022-01-05"]
     :ohjaaja_email [:s "ohjaaja@esimerkki.fi"]
     :hankkimistapa_id [:n 234]
     :oppija_oid [:s "test-oppija-oid"]
     :rahoituskausi [:s "2021-2022"]
     :tutkintonimike [:s "(\"test-tutkintonimike\")"]
     :viimeinen_vastauspvm [:s "2022-04-17"]
     :request_id [:s "test-uuid"]}})

(def expected-nippu-table
  #{{:ohjaaja_ytunnus_kj_tutkinto
     [:s "Olli Ohjaaja/123456-7/koulutustoimija-oid/testitutkinto"]
     :niputuspvm [:s "2022-02-16"]
     :tyopaikka [:s "Testi Oy"]
     :koulutuksenjarjestaja [:s "koulutustoimija-oid"]
     :ytunnus [:s "123456-7"]
     :ohjaaja [:s "Olli Ohjaaja"]
     :sms_kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]
     :tutkinto [:s "testitutkinto"]
     :kasittelytila [:s (:ei-niputettu c/kasittelytilat)]}})

(def expected-http-results
  [{:method :get
    :url "https://oph-koski.com/opiskeluoikeus/test-oo-oid"
    :options {:basic-auth ["koski-user" "koski-pwd"] :as :json}}
   {:method :get
    :url "https://oph-organisaatio.com/test-toimipiste"
    :options {:as :json}}
   {:method :get
    :url "https://oph-organisaatio.com/test-toimipiste"
    :options {:as :json}}
   {:method :post
    :url "https://oph-arvo.com/tyoelamapalaute/v1/vastaajatunnus"
    :options {:content-type "application/json"
              :body (str "{\"osa_aikaisuus\":50,\"tyopaikka\":\"Testi Oy\","
                         "\"tyopaikka_normalisoitu\":\"testi_oy\","
                         "\"vastaamisajan_alkupvm\":\"2022-02-16\","
                         "\"tyonantaja\":\"123456-7\","
                         "\"oppisopimuksen_perusta\":\"01\","
                         "\"osaamisala\":[\"test-osaamisala\"],"
                         "\"koulutustoimija_oid\":\"koulutustoimija-oid\","
                         "\"paikallinen_tutkinnon_osa\":\"Test Kurssi\","
                         "\"rahoituskausi_pvm\":\"2022-02-02\","
                         "\"tyopaikkajakson_loppupvm\":\"2022-02-02\","
                         "\"toimipiste_oid\":\"test-toimipiste\","
                         "\"oppilaitos_oid\":\"testilaitos\","
                         "\"tyopaikkajakson_alkupvm\":\"2022-01-05\","
                         "\"tutkintotunnus\":\"testitutkinto\","
                         "\"sopimustyyppi\":\"oppisopimus\","
                         "\"tutkintonimike\":[\"test-tutkintonimike\"],"
                         "\"request_id\":\"test-uuid\","
                         "\"tutkinnon_osa\":\"test-tutkinnonosa\"}")
              :basic-auth ["arvo-user" "arvo-pwd"]
              :as :json}}
   {:method :patch
    :url (str (:ehoks-url mock-env)
              "heratepalvelu/osaamisenhankkimistavat/234/kasitelty")
    :options {:headers {:ticket (str "service-ticket/ehoks-virkailija-backend"
                                     "/cas-security-check")}
              :content-type "application/json"
              :as :json}}
   {:method :get
    :url (str (:koski-url mock-env) "/opiskeluoikeus/test-oo-oid2")
    :options {:basic-auth ["koski-user" "koski-pwd"] :as :json}}
   {:method :patch
    :url (str (:ehoks-url mock-env)
              "heratepalvelu/osaamisenhankkimistavat/567/kasitelty")
    :options {:headers {:ticket (str "service-ticket/ehoks-virkailija-backend"
                                     "/cas-security-check")}
              :content-type "application/json"
              :as :json}}
   {:method :get
    :url "https://oph-koski.com/opiskeluoikeus/test-oo-oid"
    :options {:basic-auth ["koski-user" "koski-pwd"] :as :json}}
   {:method :patch
    :url (str (:ehoks-url mock-env)
              "heratepalvelu/osaamisenhankkimistavat/234/kasitelty")
    :options {:headers {:ticket (str "service-ticket/ehoks-virkailija-backend"
                                     "/cas-security-check")}
              :content-type "application/json"
              :as :json}}
   {:method :get
    :url (str (:koski-url mock-env)
              "/opiskeluoikeus/test-oo-oid-externalfunding")
    :options {:basic-auth ["koski-user" "koski-pwd"] :as :json}}
   {:method :patch
    :url (str (:ehoks-url mock-env)
              "heratepalvelu/osaamisenhankkimistavat/456/kasitelty")
    :options
    {:headers
     {:ticket "service-ticket/ehoks-virkailija-backend/cas-security-check"}
     :as :json
     :content-type "application/json"}}])

(def expected-cas-client-results
  (repeat 4 {:type :get-service-ticket
             :service "/ehoks-virkailija-backend"
             :suffix "cas-security-check"}))

(deftest test-jaksoHandler-integration
  (testing "jaksoHandler integraatiotesti"
    (with-redefs [environ.core/env mock-env
                  oph.heratepalvelu.common/generate-uuid (fn [] "test-uuid")
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 2 2))
                  oph.heratepalvelu.db.dynamodb/get-item mdb/get-item
                  oph.heratepalvelu.db.dynamodb/put-item mdb/put-item
                  oph.heratepalvelu.db.dynamodb/query-items mdb/query-items
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.cas-client/get-service-ticket
                  mcc/mock-get-service-ticket
                  oph.heratepalvelu.external.http-client/get mhc/mock-get
                  oph.heratepalvelu.external.http-client/patch mhc/mock-patch
                  oph.heratepalvelu.external.http-client/post mhc/mock-post
                  oph.heratepalvelu.external.koski/pwd (delay "koski-pwd")]
      (setup-test)
      (let [context (tu/mock-handler-context)]
        (jh/-handleJaksoHerate {} (tu/mock-sqs-event test-herate) context)
        (jh/-handleJaksoHerate {} (tu/mock-sqs-event duplicate-herate) context)
        (jh/-handleJaksoHerate
          {} (tu/mock-sqs-event test-herate-missing-osa-aikaisuus) context)
        (jh/-handleJaksoHerate
          {} (tu/mock-sqs-event test-herate-externalfunding) context))
      (is (= (mdb/get-table-values (:jaksotunnus-table mock-env))
             expected-jaksotunnus-table))
      (is (= (mdb/get-table-values (:nippu-table mock-env))
             expected-nippu-table))
      (is (= (mhc/get-results) expected-http-results)
          (->> (diff (mhc/get-results) expected-http-results)
               (clojure.string/join "\n")
               (str "Differences: ")))
      (is (= (mcc/get-results) expected-cas-client-results))
      (teardown-test))))
