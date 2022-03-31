(ns oph.heratepalvelu.integration-tests.tep.StatusHandler-i-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.integration-tests.mock-cas-client :as mcc]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.integration-tests.mock-http-client :as mhc]
            [oph.heratepalvelu.tep.StatusHandler :as sh]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(def mock-env {:nippu-table "nippu-table-name"
               :arvo-url "https://oph-arvo.com/"
               :arvo-user "arvo-user"
               :viestintapalvelu-url "https://oph-viestintapalvelu.com"})

(def starting-nippu-table
  [{:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
    :niputuspvm [:s "2022-01-16"]
    :kasittelytila [:s (:viestintapalvelussa c/kasittelytilat)]
    :viestintapalvelu-id [:n 11]
    :voimassaloppupvm [:s "2022-02-28"]
    :kyselylinkki [:s "kysely.linkki/asdf"]}
   {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1a"]
    :niputuspvm [:s "2022-01-16"]
    :kasittelytila [:s (:viestintapalvelussa c/kasittelytilat)]
    :viestintapalvelu-id [:n 12]
    :voimassaloppupvm [:s "2022-02-28"]
    :sms_kasittelytila [:s (:success c/kasittelytilat)]
    :kyselylinkki [:s "kysely.linkki/asdf_a"]}
   {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1b"]
    :niputuspvm [:s "2022-01-16"]
    :kasittelytila [:s (:viestintapalvelussa c/kasittelytilat)]
    :viestintapalvelu-id [:n 13]
    :voimassaloppupvm [:s "2022-02-28"]
    :kyselylinkki [:s "kysely.linkki/asdf_b"]}
   {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-2"]
    :niputuspvm [:s "2022-01-16"]
    :kasittelytila [:s (:viestintapalvelussa c/kasittelytilat)]
    :viestintapalvelu-id [:n 21]
    :voimassaloppupvm [:s "2022-02-28"]
    :kyselylinkki [:s "kysely.linkki/asdf,lkj"]}
   {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-3"]
    :niputuspvm [:s "2022-01-16"]
    :kasittelytila [:s (:viestintapalvelussa c/kasittelytilat)]
    :viestintapalvelu-id [:n 31]
    :voimassaloppupvm [:s "2022-02-28"]
    :kyselylinkki [:s "kysely.linkki/asdf;lkj"]}
   {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-4"]
    :niputuspvm [:s "2022-01-16"]
    :kasittelytila [:s (:no-email c/kasittelytilat)]
    :viestintapalvelu-id [:n 41]
    :voimassaloppupvm [:s "2022-02-28"]
    :kyselylinkki [:s "kysely.linkki/oiuoiu"]}])

(defn- setup-test []
  (mcc/clear-results)
  (mcc/clear-url-bindings)
  (mcc/bind-url :post
                (str (:viestintapalvelu-url mock-env) "/status")
                11
                {:body {:numberOfSuccessfulSendings 1}})
  (mcc/bind-url :post
                (str (:viestintapalvelu-url mock-env) "/status")
                12
                {:body {:numberOfSuccessfulSendings 1}})
  (mcc/bind-url :post
                (str (:viestintapalvelu-url mock-env) "/status")
                13
                {:body {:numberOfFailedSendings 1}})
  (mcc/bind-url :post
                (str (:viestintapalvelu-url mock-env) "/status")
                21
                {:body {:numberOfSuccessfulSendings 1}})
  (mcc/bind-url :post
                (str (:viestintapalvelu-url mock-env) "/status")
                31
                {:body {:numberOfSuccessfulSendings 1}})
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mdb/clear-mock-db)
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
  (mdb/clear-mock-db))

(def expected-nippu-table #{{:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
                             :niputuspvm [:s "2022-01-16"]
                             :kasittelytila [:s (:success c/kasittelytilat)]
                             :viestintapalvelu-id [:n 11]
                             :voimassaloppupvm [:s "2022-03-04"]
                             :kyselylinkki [:s "kysely.linkki/asdf"]}
                            {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1a"]
                             :niputuspvm [:s "2022-01-16"]
                             :kasittelytila [:s (:success c/kasittelytilat)]
                             :viestintapalvelu-id [:n 12]
                             :voimassaloppupvm [:s "2022-02-28"]
                             :sms_kasittelytila [:s (:success c/kasittelytilat)]
                             :kyselylinkki [:s "kysely.linkki/asdf_a"]}
                            {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1b"]
                             :niputuspvm [:s "2022-01-16"]
                             :kasittelytila [:s (:failed c/kasittelytilat)]
                             :viestintapalvelu-id [:n 13]
                             :voimassaloppupvm [:s "2022-03-04"]
                             :kyselylinkki [:s "kysely.linkki/asdf_b"]}
                            {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-2"]
                             :niputuspvm [:s "2022-01-16"]
                             :kasittelytila [:s (:success c/kasittelytilat)]
                             :viestintapalvelu-id [:n 21]
                             :voimassaloppupvm [:s "2022-03-04"]
                             :kyselylinkki [:s "kysely.linkki/asdf,lkj"]}
                            {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-3"]
                             :niputuspvm [:s "2022-01-16"]
                             :kasittelytila [:s (:success c/kasittelytilat)]
                             :viestintapalvelu-id [:n 31]
                             :voimassaloppupvm [:s "2022-03-04"]
                             :kyselylinkki [:s "kysely.linkki/asdf;lkj"]}
                            {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-4"]
                             :niputuspvm [:s "2022-01-16"]
                             :kasittelytila [:s (:no-email c/kasittelytilat)]
                             :viestintapalvelu-id [:n 41]
                             :voimassaloppupvm [:s "2022-02-28"]
                             :kyselylinkki [:s "kysely.linkki/oiuoiu"]}})

(def expected-cas-client-results [{:method :post
                                   :url (str (:viestintapalvelu-url mock-env)
                                             "/status")
                                   :body 11
                                   :options {:as :json}}
                                  {:method :post
                                   :url (str (:viestintapalvelu-url mock-env)
                                             "/status")
                                   :body 12
                                   :options {:as :json}}
                                  {:method :post
                                   :url (str (:viestintapalvelu-url mock-env)
                                             "/status")
                                   :body 13
                                   :options {:as :json}}
                                  {:method :post
                                   :url (str (:viestintapalvelu-url mock-env)
                                             "/status")
                                   :body 21
                                   :options {:as :json}}
                                  {:method :post
                                   :url (str (:viestintapalvelu-url mock-env)
                                             "/status")
                                   :body 31
                                   :options {:as :json}}])

(def expected-http-results
  [{:method :patch
    :url (str (:arvo-url mock-env) "tyoelamapalaute/v1/nippu/asdf")
    :options {:as :json
              :basic-auth [(:arvo-user mock-env) "arvo-pwd"]
              :content-type "application/json"
              :body
              "{\"tila\":\"lahetetty\",\"voimassa_loppupvm\":\"2022-03-04\"}"}}
   {:method :patch
    :url (str (:arvo-url mock-env) "tyoelamapalaute/v1/nippu/asdf_a")
    :options {:as :json
              :basic-auth ["arvo-user" "arvo-pwd"]
              :content-type "application/json"
              :body "{\"tila\":\"lahetetty\"}"}}
   {:method :patch
    :url (str (:arvo-url mock-env) "tyoelamapalaute/v1/nippu/asdf_b")
    :options {:as :json
              :basic-auth ["arvo-user" "arvo-pwd"]
              :content-type "application/json"
              :body "{\"tila\":\"lahetys_epaonnistunut\"}"}}])

(deftest test-StatusHandler-integration
  (testing "StatusHandler integraatiotesti"
    (with-redefs [environ.core/env mock-env
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 2 2))
                  oph.heratepalvelu.db.dynamodb/get-item mdb/get-item
                  oph.heratepalvelu.db.dynamodb/query-items mdb/query-items
                  oph.heratepalvelu.db.dynamodb/update-item mdb/update-item
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.cas-client/cas-authenticated-post
                  mcc/mock-cas-authenticated-post
                  oph.heratepalvelu.external.http-client/patch mhc/mock-patch]
      (setup-test)
      (sh/-handleEmailStatus {}
                             (tu/mock-handler-event :scheduledherate)
                             (tu/mock-handler-context))
      (is (= (mdb/get-table-values (:nippu-table mock-env))
             expected-nippu-table))
      (is (= (mcc/get-results) expected-cas-client-results))
      (is (= (mhc/get-results) expected-http-results))
      (teardown-test))))
