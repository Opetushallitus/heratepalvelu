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
               :arvo-user "arvo-user"
               :ehoks-url "https://oph-ehoks.com/"
               :ehoks-user "ehoks-user"
               :koski-url "https://oph-koski.com"
               :koski-user "koski-user"})

(def starting-jaksotunnus-table [{:hankkimistapa_id [:n 11]
                                  :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
                                  :niputuspvm [:s "2022-02-01"]
                                  :viimeinen_vastauspvm [:s "2022-03-31"]
                                  :tunnus [:s "AAjunk"]
                                  :oppija_oid [:s "aaa"]
                                  :opiskeluoikeus_oid [:s "oo-aaa"]
                                  :jakso_alkupvm [:s "2022-01-01"]
                                  :jakso_loppupvm [:s "2022-01-25"]
                                  :tyopaikan_nimi [:s "Testi Työpaikka 1"]}
                                 {:hankkimistapa_id [:n 12]
                                  :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
                                  :niputuspvm [:s "2022-02-01"]
                                  :viimeinen_vastauspvm [:s "2022-02-16"]
                                  :tunnus [:s "ABjunk"]
                                  :oppija_oid [:s "aaa"]
                                  :opiskeluoikeus_oid [:s "oo-aaa"]
                                  :jakso_alkupvm [:s "2022-01-15"]
                                  :jakso_loppupvm [:s "2022-01-31"]
                                  :tyopaikan_nimi [:s "Testi Työpaikka 1"]}
                                 {:hankkimistapa_id [:n 13]
                                  :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
                                  :niputuspvm [:s "2022-02-01"]
                                  :viimeinen_vastauspvm [:s "2022-02-25"]
                                  :tunnus [:s "ACjunk"]
                                  :oppija_oid [:s "aaa"]
                                  :opiskeluoikeus_oid [:s "oo-aaa"]
                                  :jakso_alkupvm [:s "2022-01-20"]
                                  :jakso_loppupvm [:s "2022-01-31"]
                                  :tyopaikan_nimi [:s "Testi Työpaikka 1"]}
                                 {:hankkimistapa_id [:n 21]
                                  :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-2"]
                                  :niputuspvm [:s "2022-02-01"]
                                  :viimeinen_vastauspvm [:s "2022-03-31"]
                                  :tunnus [:s "BAjunk"]
                                  :oppija_oid [:s "bbb"]
                                  :opiskeluoikeus_oid [:s "oo-bbb"]
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
                      "\"tunnukset\":[{\"tunnus\":\"AAjunk\","
                      "\"tyopaikkajakson_kesto\":12.167},"
                      "{\"tunnus\":\"ACjunk\","
                      "\"tyopaikkajakson_kesto\":1.667}],"
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
                      "\"tunnukset\":[{\"tunnus\":\"BAjunk\","
                      "\"tyopaikkajakson_kesto\":1.0}],"
                      "\"voimassa_alkupvm\":\"2022-02-18\","
                      "\"request_id\":\"test-uuid\"}")}
                {:body {:errors "Jokin meni pieleen"}})
  (mhc/bind-url :get
                (str (:ehoks-url mock-env)
                     "heratepalvelu/tyoelamajaksot-active-between")
                {:as :json
                 :headers
                 {:ticket
                  "service-ticket/ehoks-virkailija-backend/cas-security-check"}
                 :query-params {:oppija "aaa"
                                :start "2022-01-01"
                                :end "2022-01-31"}}
                [{:hankkimistapa_id 11
                  :ohjaaja_ytunnus_kj_tutkinto "oykt-1"
                  :niputuspvm "2022-02-01"
                  :viimeinen_vastauspvm "2022-03-31"
                  :tunnus "AAjunk"
                  :oppija_oid "aaa"
                  :opiskeluoikeus_oid "oo-aaa"
                  :jakso_alkupvm "2022-01-01"
                  :jakso_loppupvm "2022-01-25"
                  :tyopaikan_nimi "Testi Työpaikka 1"}
                 {:hankkimistapa_id 12
                  :ohjaaja_ytunnus_kj_tutkinto "oykt-1"
                  :niputuspvm "2022-02-01"
                  :viimeinen_vastauspvm "2022-02-16"
                  :tunnus "ABjunk"
                  :oppija_oid "aaa"
                  :opiskeluoikeus_oid "oo-aaa"
                  :jakso_alkupvm "2022-01-15"
                  :jakso_loppupvm "2022-01-31"
                  :tyopaikan_nimi "Testi Työpaikka 1"}
                 {:hankkimistapa_id 13
                  :ohjaaja_ytunnus_kj_tutkinto "oykt-1"
                  :niputuspvm "2022-02-01"
                  :viimeinen_vastauspvm "2022-02-25"
                  :tunnus "ACjunk"
                  :oppija_oid "aaa"
                  :opiskeluoikeus_oid "oo-aaa"
                  :jakso_alkupvm "2022-01-20"
                  :jakso_loppupvm "2022-01-31"
                  :tyopaikan_nimi "Testi Työpaikka 1"}])
  (mhc/bind-url :get
                (str (:ehoks-url mock-env)
                     "heratepalvelu/tyoelamajaksot-active-between")
                {:as :json
                 :headers
                 {:ticket
                  "service-ticket/ehoks-virkailija-backend/cas-security-check"}
                 :query-params {:oppija "bbb"
                                :start "2022-01-10"
                                :end "2022-01-20"}}
                [{:hankkimistapa_id 21
                  :ohjaaja_ytunnus_kj_tutkinto "oykt-2"
                  :niputuspvm "2022-02-01"
                  :viimeinen_vastauspvm "2022-03-31"
                  :tunnus "BAjunk"
                  :oppija_oid "bbb"
                  :opiskeluoikeus_oid "oo-bbb"
                  :keskeytymisajanjaksot [{:alku "2022-01-12"
                                           :loppu "2022-01-14"}]
                  :jakso_alkupvm "2022-01-10"
                  :jakso_loppupvm "2022-01-20"
                  :tyopaikan_nimi "Testi Työpaikka 2"}
                 {:hankkimistapa_id 22
                  :ohjaaja_ytunnus_kj_tutkinto "oykt-2"
                  :tunnus "BBjunk"
                  :oppija_oid "bbb"
                  :opiskeluoikeus_oid "oo-bbb"
                  :keskeytymisajanjaksot [{:alku "2022-01-03"
                                           :loppu "2022-01-08"}]
                  :jakso_alkupvm "2022-01-01"
                  :jakso_loppupvm "2022-01-30"
                  :tyopaikan_nimi "Testi Työpaikka 2"}])
  (mhc/bind-url :get
                (str (:koski-url mock-env) "/opiskeluoikeus/oo-aaa")
                {:as         :json
                 :basic-auth [(:koski-user mock-env) "koski-pwd"]}
                {:body {:tila
                        {:opiskeluoikeusjaksot
                         [{:alku "2021-09-01" :tila {:koodiarvo "lasna"}}
                          {:alku "2022-01-24" :tila {:koodiarvo "loma"}}
                          {:alku "2022-01-28" :tila {:koodiarvo "lasna"}}]}}})
  (mhc/bind-url :get
                (str (:koski-url mock-env) "/opiskeluoikeus/oo-bbb")
                {:as         :json
                 :basic-auth [(:koski-user mock-env) "koski-pwd"]}
                {:body {:tila
                        {:opiskeluoikeusjaksot
                         [{:alku "2021-09-01" :tila {:koodiarvo "lasna"}}
                          {:alku "2022-01-17" :tila {:koodiarvo "loma"}}
                          {:alku "2022-01-31" :tila {:koodiarvo "lasna"}}]}}})
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

(def expected-jaksotunnus-table #{{:hankkimistapa_id [:n 11]
                                   :kesto [:n 12.167]
                                   :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
                                   :niputuspvm [:s "2022-02-01"]
                                   :viimeinen_vastauspvm [:s "2022-03-31"]
                                   :tunnus [:s "AAjunk"]
                                   :oppija_oid [:s "aaa"]
                                   :opiskeluoikeus_oid [:s "oo-aaa"]
                                   :jakso_alkupvm [:s "2022-01-01"]
                                   :jakso_loppupvm [:s "2022-01-25"]
                                   :tyopaikan_nimi [:s "Testi Työpaikka 1"]}
                                  {:hankkimistapa_id [:n 12]
                                   :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
                                   :niputuspvm [:s "2022-02-01"]
                                   :viimeinen_vastauspvm [:s "2022-02-16"]
                                   :tunnus [:s "ABjunk"]
                                   :oppija_oid [:s "aaa"]
                                   :opiskeluoikeus_oid [:s "oo-aaa"]
                                   :jakso_alkupvm [:s "2022-01-15"]
                                   :jakso_loppupvm [:s "2022-01-31"]
                                   :tyopaikan_nimi [:s "Testi Työpaikka 1"]}
                                  {:hankkimistapa_id [:n 13]
                                   :kesto [:n 1.667]
                                   :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
                                   :niputuspvm [:s "2022-02-01"]
                                   :viimeinen_vastauspvm [:s "2022-02-25"]
                                   :tunnus [:s "ACjunk"]
                                   :oppija_oid [:s "aaa"]
                                   :opiskeluoikeus_oid [:s "oo-aaa"]
                                   :jakso_alkupvm [:s "2022-01-20"]
                                   :jakso_loppupvm [:s "2022-01-31"]
                                   :tyopaikan_nimi [:s "Testi Työpaikka 1"]}
                                  {:hankkimistapa_id [:n 21]
                                   :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-2"]
                                   :kesto [:n 1.0]
                                   :niputuspvm [:s "2022-02-01"]
                                   :viimeinen_vastauspvm [:s "2022-03-31"]
                                   :tunnus [:s "BAjunk"]
                                   :oppija_oid [:s "bbb"]
                                   :opiskeluoikeus_oid [:s "oo-bbb"]
                                   :jakso_alkupvm [:s "2022-01-10"]
                                   :jakso_loppupvm [:s "2022-01-20"]
                                   :tyopaikan_nimi [:s "Testi Työpaikka 2"]}})

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
  [{:method :get
    :url "https://oph-ehoks.com/heratepalvelu/tyoelamajaksot-active-between"
    :options {:headers
              {:ticket
               "service-ticket/ehoks-virkailija-backend/cas-security-check"}
              :as :json
              :query-params {:oppija "aaa"
                             :start "2022-01-01"
                             :end "2022-01-31"}}}
   {:method :get
    :url "https://oph-koski.com/opiskeluoikeus/oo-aaa"
    :options {:basic-auth ["koski-user" "koski-pwd"]
              :as :json}}
   {:method :post
    :url "https://oph-arvo.com/tyoelamapalaute/v1/nippu"
    :options {:content-type "application/json"
              :body (str "{\"tunniste\":\"testi_tyopaikka_1_2022-02-18_test6\","
                         "\"koulutustoimija_oid\":\"test-kj-1\","
                         "\"tutkintotunnus\":\"test-tutkinto-1\","
                         "\"tyonantaja\":\"123456-1\","
                         "\"tyopaikka\":\"Testi Työpaikka 1\","
                         "\"tunnukset\":[{\"tunnus\":\"AAjunk\","
                         "\"tyopaikkajakson_kesto\":12.167},"
                         "{\"tunnus\":\"ACjunk\","
                         "\"tyopaikkajakson_kesto\":1.667}],"
                         "\"voimassa_alkupvm\":\"2022-02-18\","
                         "\"request_id\":\"test-uuid\"}")
              :basic-auth ["arvo-user" "arvo-pwd"] :as :json}}
   {:method :get
    :url "https://oph-ehoks.com/heratepalvelu/tyoelamajaksot-active-between"
    :options {:headers
              {:ticket
               "service-ticket/ehoks-virkailija-backend/cas-security-check"}
              :as :json
              :query-params {:oppija "bbb"
                             :start "2022-01-10"
                             :end "2022-01-20"}}}
   {:method :get
    :url "https://oph-koski.com/opiskeluoikeus/oo-bbb"
    :options {:basic-auth ["koski-user" "koski-pwd"]
              :as :json}}
   {:method :post
    :url "https://oph-arvo.com/tyoelamapalaute/v1/nippu"
    :options {:content-type "application/json"
              :body (str "{\"tunniste\":\"testi_tyopaikka_2_2022-02-18_test6\","
                         "\"koulutustoimija_oid\":\"test-kj-2\","
                         "\"tutkintotunnus\":\"test-tutkinto-2\","
                         "\"tyonantaja\":\"123456-2\","
                         "\"tyopaikka\":\"Testi Työpaikka 2\","
                         "\"tunnukset\":[{\"tunnus\":\"BAjunk\","
                         "\"tyopaikkajakson_kesto\":1.0}],"
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
