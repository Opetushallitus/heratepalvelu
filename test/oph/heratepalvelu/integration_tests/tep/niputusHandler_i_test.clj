(ns oph.heratepalvelu.integration-tests.tep.niputusHandler-i-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.integration-tests.mock-cas-client :as mcc]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.integration-tests.mock-http-client :as mhc]
            [oph.heratepalvelu.tep.niputusHandler :as nh]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(def mock-env {:jaksotunnus-table "jaksotunnus-table-name"
               :nippu-table "nippu-table-name"
               :arvo-url "https://oph-arvo.com/"
               :arvo-user "arvo-user"})

;; TODO mock koski calls (näistä pitäisi olla... 3?)
;; TODO mock ehoks calls (get-tyoelamajaksot-active-between) (2)
;; TODO alku ja loppu jokaiseen jaksoon

(def starting-jaksotunnus-table [{:hankkimistapa_id [:n 11]
                                  :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
                                  :niputuspvm [:s "2022-02-01"]
                                  :viimeinen_vastauspvm [:s "2022-03-31"]
                                  :tunnus [:s "AAjunk"]
                                  :oppija_oid [:s "aaa"]
                                  :jakso_alkupvm [:s "2022-01-01"]
                                  :jakso_loppupvm [:s "2022-01-25"]
                                  :tyopaikan_nimi [:s "Testi Työpaikka 1"]}
                                 {:hankkimistapa_id [:n 12]
                                  :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
                                  :niputuspvm [:s "2022-02-01"]
                                  :viimeinen_vastauspvm [:s "2022-02-16"]
                                  :tunnus [:s "ABjunk"]
                                  :oppija_oid [:s "aaa"]
                                  :jakso_alkupvm [:s "2022-01-15"]
                                  :jakso_loppupvm [:s "2022-01-31"]
                                  :tyopaikan_nimi [:s "Testi Työpaikka 1"]}
                                 {:hankkimistapa_id [:n 13]
                                  :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
                                  :niputuspvm [:s "2022-02-01"]
                                  :viimeinen_vastauspvm [:s "2022-02-25"]
                                  :tunnus [:s "ACjunk"]
                                  :oppija_oid [:s "aaa"]
                                  :jakso_alkupvm [:s "2022-01-20"]
                                  :jakso_loppupvm [:s "2022-01-31"]
                                  :tyopaikan_nimi [:s "Testi Työpaikka 1"]}
                                 {:hankkimistapa_id [:n 21]
                                  :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-2"]
                                  :niputuspvm [:s "2022-02-01"]
                                  :viimeinen_vastauspvm [:s "2022-03-31"]
                                  :tunnus [:s "BAjunk"]
                                  :oppija_oid [:s "bbb"]
                                  :jakso_alkupvm [:s "2022-01-10"]
                                  :jakso_loppupvm [:s "2022-01-20"]
                                  :tyopaikan_nimi [:s "Testi Työpaikka 2"]}])

(def starting-nippu-table [{:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
                            :niputuspvm [:s "2022-02-01"]
                            :kasittelytila [:s (:ei-niputettu c/kasittelytilat)]
                            :tyopaikka [:s "Testi Työpaikka 1"]
                            :ytunnus [:s "123456-1"]
                            :tutkinto [:s "test-tutkinto-1"]
                            :koulutuksenjarjestaja [:s "test-kj-1"]}
                           {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-2"]
                            :niputuspvm [:s "2022-02-01"]
                            :kasittelytila [:s (:ei-niputettu c/kasittelytilat)]
                            :tyopaikka [:s "Testi Työpaikka 2"]
                            :ytunnus [:s "123456-2"]
                            :tutkinto [:s "test-tutkinto-2"]
                            :koulutuksenjarjestaja [:s "test-kj-2"]}
                           {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-3"]
                            :niputuspvm [:s "2022-02-01"]
                            :kasittelytila [:s (:ei-niputettu c/kasittelytilat)]
                            :tyopaikka [:s "Testi Työpaikka 3"]
                            :ytunnus [:s "123456-3"]
                            :tutkinto [:s "test-tutkinto-3"]
                            :koulutuksenjarjestaja [:s "test-kj-3"]}])

(defn- setup-test []
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mhc/bind-url :post
                (str (:arvo-url mock-env) "tyoelamapalaute/v1/nippu")
                {:content-type "application/json"
                 :as :json
                 :basic-auth [(:arvo-user mock-env) "arvo-pwd"]
                 :body
                 (str "{\"tunniste\":\"testi_tyopaikka_1_2022-02-18_test6\","
                      "\"koulutustoimija_oid\":\"test-kj-1\","
                      "\"tutkintotunnus\":\"test-tutkinto-1\","
                      "\"tyonantaja\":\"123456-1\","
                      "\"tyopaikka\":\"Testi Työpaikka 1\","
                      "\"tunnukset\":[\"AAjunk\"],"
                      "\"voimassa_alkupvm\":\"2022-02-18\","
                      "\"request_id\":\"test-uuid\"}")}
                {:body {:nippulinkki "kysely.linkki/123"
                        :voimassa_loppupvm "2022-03-03"}})
  (mhc/bind-url :post
                (str (:arvo-url mock-env) "tyoelamapalaute/v1/nippu")
                {:content-type "application/json"
                 :as :json
                 :basic-auth [(:arvo-user mock-env) "arvo-pwd"]
                 :body
                 (str "{\"tunniste\":\"testi_tyopaikka_2_2022-02-18_test6\","
                      "\"koulutustoimija_oid\":\"test-kj-2\","
                      "\"tutkintotunnus\":\"test-tutkinto-2\","
                      "\"tyonantaja\":\"123456-2\","
                      "\"tyopaikka\":\"Testi Työpaikka 2\","
                      "\"tunnukset\":[\"BAjunk\"],"
                      "\"voimassa_alkupvm\":\"2022-02-18\","
                      "\"request_id\":\"test-uuid\"}")}
                {:body {:errors "Jokin meni pieleen"}})
  (mdb/clear-mock-db)
  (mdb/create-table (:jaksotunnus-table mock-env)
                    {:primary-key :hankkimistapa_id})
  (mdb/set-table-contents (:jaksotunnus-table mock-env)
                          starting-jaksotunnus-table)
  (mdb/add-index-key-fields (:jaksotunnus-table mock-env)
                            "niputusIndex"
                            {:primary-key :ohjaaja_ytunnus_kj_tutkinto
                             :sort-key :niputuspvm})
  (mdb/create-table (:nippu-table mock-env)
                    {:primary-key :ohjaaja_ytunnus_kj_tutkinto
                     :sort-key :niputuspvm})
  (mdb/set-table-contents (:nippu-table mock-env) starting-nippu-table)
  (mdb/add-index-key-fields (:nippu-table mock-env)
                            "niputusIndex"
                            {:primary-key :kasittelytila
                             :sort-key :niputuspvm}))

(defn- teardown-test []
  (mcc/clear-results)
  (mcc/clear-url-bindings)
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mdb/clear-mock-db))

(def expected-jaksotunnus-table (into #{} starting-jaksotunnus-table))

(def expected-nippu-table
  #{{:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
     :niputuspvm [:s "2022-02-01"]
     :tyopaikka [:s "Testi Työpaikka 1"]
     :kyselylinkki [:s "kysely.linkki/123"]
     :kasittelypvm [:s "2022-02-18"]
     :koulutuksenjarjestaja [:s "test-kj-1"]
     :ytunnus [:s "123456-1"]
     :voimassaloppupvm [:s "2022-03-03"]
     :tutkinto [:s "test-tutkinto-1"]
     :request_id [:s "test-uuid"]
     :kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]}
    {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-2"]
     :niputuspvm [:s "2022-02-01"]
     :tyopaikka [:s "Testi Työpaikka 2"]
     :kasittelypvm [:s "2022-02-18"]
     :koulutuksenjarjestaja [:s "test-kj-2"]
     :ytunnus [:s "123456-2"]
     :reason [:s "Jokin meni pieleen"]
     :tutkinto [:s "test-tutkinto-2"]
     :request_id [:s "test-uuid"]
     :kasittelytila [:s (:niputusvirhe c/kasittelytilat)]}
    {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-3"]
     :niputuspvm [:s "2022-02-01"]
     :tyopaikka [:s "Testi Työpaikka 3"]
     :kasittelypvm [:s "2022-02-18"]
     :koulutuksenjarjestaja [:s "test-kj-3"]
     :ytunnus [:s "123456-3"]
     :tutkinto [:s "test-tutkinto-3"]
     :request_id [:s "test-uuid"]
     :kasittelytila [:s (:ei-jaksoja c/kasittelytilat)]}})

(def expected-http-results
  [{:method :post
    :url "https://oph-arvo.com/tyoelamapalaute/v1/nippu"
    :options {:content-type "application/json"
              :body (str "{\"tunniste\":\"testi_tyopaikka_1_2022-02-18_test6\","
                         "\"koulutustoimija_oid\":\"test-kj-1\","
                         "\"tutkintotunnus\":\"test-tutkinto-1\","
                         "\"tyonantaja\":\"123456-1\","
                         "\"tyopaikka\":\"Testi Työpaikka 1\","
                         "\"tunnukset\":[\"AAjunk\"],"
                         "\"voimassa_alkupvm\":\"2022-02-18\","
                         "\"request_id\":\"test-uuid\"}")
              :basic-auth ["arvo-user" "arvo-pwd"] :as :json}}
   {:method :post
    :url "https://oph-arvo.com/tyoelamapalaute/v1/nippu"
    :options {:content-type "application/json"
              :body (str "{\"tunniste\":\"testi_tyopaikka_2_2022-02-18_test6\","
                         "\"koulutustoimija_oid\":\"test-kj-2\","
                         "\"tutkintotunnus\":\"test-tutkinto-2\","
                         "\"tyonantaja\":\"123456-2\","
                         "\"tyopaikka\":\"Testi Työpaikka 2\","
                         "\"tunnukset\":[\"BAjunk\"],"
                         "\"voimassa_alkupvm\":\"2022-02-18\","
                         "\"request_id\":\"test-uuid\"}")
              :basic-auth ["arvo-user" "arvo-pwd"] :as :json}}])

(deftest test-niputusHandler-integration
  (testing "niputusHandler integraatiotesti"
    (with-redefs [environ.core/env mock-env
                  oph.heratepalvelu.common/generate-uuid (fn [] "test-uuid")
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 2 18))
                  oph.heratepalvelu.common/rand-str (fn [len] (str "test" len))
                  oph.heratepalvelu.db.dynamodb/query-items mdb/query-items
                  oph.heratepalvelu.db.dynamodb/update-item mdb/update-item
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.cas-client/get-service-ticket
                  mcc/mock-get-service-ticket
                  oph.heratepalvelu.external.http-client/get mhc/mock-get
                  oph.heratepalvelu.external.http-client/post mhc/mock-post
                  oph.heratepalvelu.external.koski/pwd (delay "koski-pwd")]
      (setup-test)
      (nh/-handleNiputus {}
                         (tu/mock-handler-event :scheduledherate)
                         (tu/mock-handler-context))
      (is (= (mdb/get-table-values (:jaksotunnus-table mock-env))
             expected-jaksotunnus-table))
      (is (= (mdb/get-table-values (:nippu-table mock-env))
             expected-nippu-table))
      (is (= (mhc/get-results) expected-http-results))
      (teardown-test))))
