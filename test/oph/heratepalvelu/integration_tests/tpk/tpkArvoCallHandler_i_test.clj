(ns oph.heratepalvelu.integration-tests.tpk.tpkArvoCallHandler-i-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.integration-tests.mock-http-client :as mhc]
            [oph.heratepalvelu.tpk.tpkArvoCallHandler :as tach]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(def mock-env {:tpk-nippu-table "tpk-nippu-table-name"
               :arvo-url "https://oph-arvo.com/"
               :arvo-user "arvo-user"})

(def starting-table
  [{:nippu-id [:s "tyo_paikka/123456-7/test-kt/2021-07-01_2021-12-31"]
    :tyopaikan-nimi [:s "Työ Paikka"]
    :tyopaikan-nimi-normalisoitu [:s "tyo_paikka"]
    :tyopaikan-ytunnus [:s "123456-7"]
    :koulutustoimija-oid [:s "test-kt"]
    :tiedonkeruu-alkupvm [:s "2021-07-01"]
    :tiedonkeruu-loppupvm [:s "2021-12-31"]
    :vastaamisajan-alkupvm [:s "2022-01-01"]
    :vastaamisajan-loppupvm [:s "2022-02-28"]
    :niputuspvm [:s "2022-01-01"]}])

(defn- setup-test []
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mhc/bind-url :post
                (str (:arvo-url mock-env)
                     "tyoelamapalaute/v1/tyopaikkakysely-tunnus")
                {:content-type "application/json"
                 :body (str "{\"tyopaikka\":\"Työ Paikka\","
                            "\"tyopaikka_normalisoitu\":\"tyo_paikka\","
                            "\"vastaamisajan_alkupvm\":\"2022-01-01\","
                            "\"tyonantaja\":\"123456-7\","
                            "\"koulutustoimija_oid\":\"test-kt\","
                            "\"tiedonkeruu_loppupvm\":\"2021-12-31\","
                            "\"tiedonkeruu_alkupvm\":\"2021-07-01\","
                            "\"request_id\":\"test-uuid\","
                            "\"vastaamisajan_loppupvm\":\"2022-02-28\"}")
                 :basic-auth   [(:arvo-user mock-env) "arvo-pwd"]
                 :as           :json}
                {:body {:kysely_linkki "kysely.linkki/ABC"
                        :tunnus "ABC"
                        :voimassa_loppupvm "2022-02-28"}})
  (mdb/clear-mock-db)
  (mdb/create-table (:tpk-nippu-table mock-env)
                    {:primary-key :nippu-id
                     :sort-key :tiedonkeruu-alkupvm})
  (mdb/set-table-contents (:tpk-nippu-table mock-env) starting-table))

(defn- teardown-test []
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mdb/clear-mock-db))

(def expected-table
  #{{:nippu-id [:s "tyo_paikka/123456-7/test-kt/2021-07-01_2021-12-31"]
     :request-id [:s "test-uuid"]
     :tyopaikan-nimi [:s "Työ Paikka"]
     :tyopaikan-nimi-normalisoitu [:s "tyo_paikka"]
     :tyopaikan-ytunnus [:s "123456-7"]
     :tunnus [:s "ABC"]
     :kyselylinkki [:s "kysely.linkki/ABC"]
     :koulutustoimija-oid [:s "test-kt"]
     :voimassa-loppupvm [:s "2022-02-28"]
     :tiedonkeruu-alkupvm [:s "2021-07-01"]
     :tiedonkeruu-loppupvm [:s "2021-12-31"]
     :vastaamisajan-alkupvm [:s "2022-01-01"]
     :vastaamisajan-loppupvm [:s "2022-02-28"]
     :niputuspvm [:s "2022-01-01"]}})

(def expected-http-results
  [{:method :post
    :url (str (:arvo-url mock-env) "tyoelamapalaute/v1/tyopaikkakysely-tunnus")
    :options {:content-type "application/json"
              :body (str "{\"tyopaikka\":\"Työ Paikka\","
                         "\"tyopaikka_normalisoitu\":\"tyo_paikka\","
                         "\"vastaamisajan_alkupvm\":\"2022-01-01\","
                         "\"tyonantaja\":\"123456-7\","
                         "\"koulutustoimija_oid\":\"test-kt\","
                         "\"tiedonkeruu_loppupvm\":\"2021-12-31\","
                         "\"tiedonkeruu_alkupvm\":\"2021-07-01\","
                         "\"request_id\":\"test-uuid\","
                         "\"vastaamisajan_loppupvm\":\"2022-02-28\"}")
              :basic-auth ["arvo-user" "arvo-pwd"]
              :as :json}}])

(deftest test-tpkArvoCallHandler-integration
  (testing "tpkArvoCallHandler integraatiotesti"
    (with-redefs [environ.core/env mock-env
                  oph.heratepalvelu.common/generate-uuid (fn [] "test-uuid")
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 2 2))
                  oph.heratepalvelu.db.dynamodb/scan mdb/scan
                  oph.heratepalvelu.db.dynamodb/update-item mdb/update-item
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.http-client/post mhc/mock-post]
      (setup-test)
      (tach/-handleTpkArvoCalls {}
                                (tu/mock-handler-event :scheduledherate)
                                (tu/mock-handler-context 40000))
      (is (= (mdb/get-table-values (:tpk-nippu-table mock-env)) expected-table))
      (is (= (mhc/get-results) expected-http-results))
      (teardown-test))))
