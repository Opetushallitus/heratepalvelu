(ns oph.heratepalvelu.integration-tests.tpk.tpkNiputusHandler-i-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.integration-tests.mock-http-client :as mhc]
            [oph.heratepalvelu.tpk.tpkNiputusHandler :as tnh]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(def mock-env {:jaksotunnus-table "jaksotunnus-table-name"
               :tpk-nippu-table "tpk-nippu-table-name"
               :arvo-url "https://oph-arvo.com/"
               :arvo-user "arvo-user"})

(def starting-jaksotunnus-table [{:hankkimistapa_id [:n 123]
                                  :tyopaikan_nimi [:s "Työ Paikka"]
                                  :tyopaikan_ytunnus [:s "123456-7"]
                                  :koulutustoimija [:s "test-kt"]
                                  :hankkimistapa_tyyppi [:s "koulutussopimus"]
                                  :jakso_loppupvm [:s "2021-09-09"]
                                  :tpk-niputuspvm [:s "ei_maaritelty"]}
                                 {:hankkimistapa_id [:n 124]
                                  :tyopaikan_nimi [:s "Työ Paikka"]
                                  :tyopaikan_ytunnus [:s "123456-7"]
                                  :koulutustoimija [:s "test-kt"]
                                  :hankkimistapa_tyyppi [:s "oppisopimus"]
                                  :oppisopimuksen_perusta [:s "01"]
                                  :jakso_loppupvm [:s "2021-09-09"]
                                  :tpk-niputuspvm [:s "ei_maaritelty"]}
                                 {:hankkimistapa_id [:n 222]
                                  :tyopaikan_nimi [:s "Ääkköset"]
                                  :tyopaikan_ytunnus [:s "333333-7"]
                                  :koulutustoimija [:s "test-kt2"]
                                  :hankkimistapa_tyyppi [:s "koulutussopimus"]
                                  :jakso_loppupvm [:s "2021-08-01"]
                                  :tpk-niputuspvm [:s "ei_maaritelty"]}])

(def starting-tpk-nippu-table
  [{:nippu-id [:s "aakkoset/333333-7/test-kt2/2021-07-01_2021-12-31"]
    :request-id [:s "test-uuid"]
    :tyopaikan-nimi [:s "Ääkköset"]
    :tyopaikan-nimi-normalisoitu [:s "aakkoset"]
    :tyopaikan-ytunnus [:s "333333-7"]
    :tunnus [:s "XYZ"]
    :kyselylinkki [:s "kysely.linkki/XYZ"]
    :koulutustoimija-oid [:s "test-kt2"]
    :voimassa-loppupvm [:s "2022-02-28"]
    :tiedonkeruu-alkupvm [:s "2021-07-01"]
    :tiedonkeruu-loppupvm [:s "2021-12-31"]
    :vastaamisajan-alkupvm [:s "2022-01-15"]
    :vastaamisajan-loppupvm [:s "2022-02-28"]
    :niputuspvm [:s "2021-07-25"]}])

(defn- setup-test []
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mhc/bind-url :post
                (str (:arvo-url mock-env)
                     "tyoelamapalaute/v1/tyopaikkakysely-tunnus")
                {:content-type "application/json"
                 :body (str "{\"tyopaikka\":\"Työ Paikka\","
                            "\"tyopaikka_normalisoitu\":\"tyo_paikka\","
                            "\"vastaamisajan_alkupvm\":\"2022-01-15\","
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
  (mdb/create-table (:jaksotunnus-table mock-env)
                    {:primary-key :hankkimistapa_id})
  (mdb/set-table-contents (:jaksotunnus-table mock-env)
                          starting-jaksotunnus-table)
  (mdb/create-table (:tpk-nippu-table mock-env)
                    {:primary-key :nippu-id
                     :sort-key :tiedonkeruu-alkupvm})
  (mdb/set-table-contents (:tpk-nippu-table mock-env)
                          starting-tpk-nippu-table))

(defn- teardown-test []
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mdb/clear-mock-db))

(def expected-jaksotunnus-table #{{:hankkimistapa_id [:n 123]
                                   :tyopaikan_nimi [:s "Työ Paikka"]
                                   :tyopaikan_ytunnus [:s "123456-7"]
                                   :koulutustoimija [:s "test-kt"]
                                   :hankkimistapa_tyyppi [:s "koulutussopimus"]
                                   :jakso_loppupvm [:s "2021-09-09"]
                                   :tpk-niputuspvm [:s "2022-01-01"]}
                                  {:hankkimistapa_id [:n 124]
                                   :tyopaikan_nimi [:s "Työ Paikka"]
                                   :tyopaikan_ytunnus [:s "123456-7"]
                                   :koulutustoimija [:s "test-kt"]
                                   :hankkimistapa_tyyppi [:s "oppisopimus"]
                                   :oppisopimuksen_perusta [:s "01"]
                                   :jakso_loppupvm [:s "2021-09-09"]
                                   :tpk-niputuspvm [:s "2022-01-01"]}
                                  {:hankkimistapa_id [:n 222]
                                   :tyopaikan_nimi [:s "Ääkköset"]
                                   :tyopaikan_ytunnus [:s "333333-7"]
                                   :koulutustoimija [:s "test-kt2"]
                                   :hankkimistapa_tyyppi [:s "koulutussopimus"]
                                   :jakso_loppupvm [:s "2021-08-01"]
                                   :tpk-niputuspvm [:s "2021-07-25"]}})

(def expected-tpk-nippu-table
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
     :vastaamisajan-alkupvm [:s "2022-01-15"]
     :vastaamisajan-loppupvm [:s "2022-02-28"]
     :niputuspvm [:s "2022-01-01"]}
    {:nippu-id [:s "aakkoset/333333-7/test-kt2/2021-07-01_2021-12-31"]
     :request-id [:s "test-uuid"]
     :tyopaikan-nimi [:s "Ääkköset"]
     :tyopaikan-nimi-normalisoitu [:s "aakkoset"]
     :tyopaikan-ytunnus [:s "333333-7"]
     :tunnus [:s "XYZ"]
     :kyselylinkki [:s "kysely.linkki/XYZ"]
     :koulutustoimija-oid [:s "test-kt2"]
     :voimassa-loppupvm [:s "2022-02-28"]
     :tiedonkeruu-alkupvm [:s "2021-07-01"]
     :tiedonkeruu-loppupvm [:s "2021-12-31"]
     :vastaamisajan-alkupvm [:s "2022-01-15"]
     :vastaamisajan-loppupvm [:s "2022-02-28"]
     :niputuspvm [:s "2021-07-25"]}})

(def expected-http-results
  [{:method :post
    :url (str (:arvo-url mock-env) "tyoelamapalaute/v1/tyopaikkakysely-tunnus")
    :options {:content-type "application/json"
              :body (str "{\"tyopaikka\":\"Työ Paikka\","
                         "\"tyopaikka_normalisoitu\":\"tyo_paikka\","
                         "\"vastaamisajan_alkupvm\":\"2022-01-15\","
                         "\"tyonantaja\":\"123456-7\","
                         "\"koulutustoimija_oid\":\"test-kt\","
                         "\"tiedonkeruu_loppupvm\":\"2021-12-31\","
                         "\"tiedonkeruu_alkupvm\":\"2021-07-01\","
                         "\"request_id\":\"test-uuid\","
                         "\"vastaamisajan_loppupvm\":\"2022-02-28\"}")
              :basic-auth ["arvo-user" "arvo-pwd"]
              :as :json}}])

(deftest test-tpkNiputusHandler-integration
  (testing "tpkNiputusHandler integraatiotesti"
    (with-redefs [environ.core/env mock-env
                  oph.heratepalvelu.common/generate-uuid (fn [] "test-uuid")
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 1 1))
                  oph.heratepalvelu.db.dynamodb/get-item mdb/get-item
                  oph.heratepalvelu.db.dynamodb/put-item mdb/put-item
                  oph.heratepalvelu.db.dynamodb/scan mdb/scan
                  oph.heratepalvelu.db.dynamodb/update-item mdb/update-item
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.http-client/post mhc/mock-post
                  oph.heratepalvelu.tpk.tpkNiputusHandler/current-kausi-end
                  (LocalDate/of 2021 12 31)
                  oph.heratepalvelu.tpk.tpkNiputusHandler/current-kausi-start
                  (LocalDate/of 2021 7 1)]
      (setup-test)
      (tnh/-handleTpkNiputus {}
                             (tu/mock-handler-event :scheduledherate)
                             (tu/mock-handler-context 40000))
      (is (= (mdb/get-table-values (:jaksotunnus-table mock-env))
             expected-jaksotunnus-table))
      (is (= (mdb/get-table-values (:tpk-nippu-table mock-env))
             expected-tpk-nippu-table))
      (is (= (mhc/get-results) expected-http-results))
      (teardown-test))))
