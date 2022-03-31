(ns oph.heratepalvelu.integration-tests.tep.tepSmsHandler-i-test
  (:require [cheshire.core :refer [generate-string]]
            [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.elisa :as elisa]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.integration-tests.mock-http-client :as mhc]
            [oph.heratepalvelu.tep.tepSmsHandler :as tsh]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(def mock-env {:jaksotunnus-table "jaksotunnus-table-name"
               :nippu-table "nippu-table-name"
               :send-messages "true"
               :arvo-url "https://oph-arvo.com/"
               :arvo-user "arvo-user"})

(def starting-jaksotunnus-table [{:hankkimistapa_id [:n 11]
                                  :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
                                  :niputuspvm [:s "2022-01-27"]
                                  :oppilaitos [:s "testilaitos"]
                                  :ohjaaja_puhelinnumero [:s "0401234567"]}
                                 {:hankkimistapa_id [:n 21]
                                  :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-2"]
                                  :niputuspvm [:s "2022-01-27"]
                                  :oppilaitos [:s "testilaitos"]
                                  :ohjaaja_puhelinnumero [:s "00000"]}
                                 {:hankkimistapa_id [:n 31]
                                  :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-3"]
                                  :niputuspvm [:s "2022-01-27"]
                                  :oppilaitos [:s "testilaitos"]
                                  :ohjaaja_puhelinnumero [:s "00000"]}
                                 {:hankkimistapa_id [:n 51]
                                  :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-5"]
                                  :niputuspvm [:s "2022-01-27"]
                                  :oppilaitos [:s "testilaitos"]
                                  :ohjaaja_puhelinnumero [:s "0401234565"]}
                                 {:hankkimistapa_id [:n 52]
                                  :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-5"]
                                  :niputuspvm [:s "2022-01-27"]
                                  :oppilaitos [:s "testilaitos"]
                                  :ohjaaja_puhelinnumero [:s "0401234560"]}])

(def starting-nippu-table
  [{:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
    :niputuspvm [:s "2022-01-27"]
    :kyselylinkki [:s "kysely.linkki/1"]
    :kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]
    :sms_kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]
    :voimassaloppupvm [:s "2022-02-28"]}
   {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-2"]
    :niputuspvm [:s "2022-01-27"]
    :kyselylinkki [:s "kysely.linkki/2"]
    :kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]
    :sms_kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]
    :voimassaloppupvm [:s "2022-02-28"]}
   {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-3"]
    :niputuspvm [:s "2022-01-27"]
    :kyselylinkki [:s "kysely.linkki/3"]
    :kasittelytila [:s (:no-email c/kasittelytilat)]
    :sms_kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]
    :voimassaloppupvm [:s "2022-02-28"]}
   {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-4"]
    :niputuspvm [:s "2022-01-27"]
    :kyselylinkki [:s "kysely.linkki/4"]
    :kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]
    :sms_kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]
    :voimassaloppupvm [:s "2022-02-28"]}
   {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-5"]
    :niputuspvm [:s "2022-01-27"]
    :kyselylinkki [:s "kysely.linkki/5"]
    :kasittelytila [:s (:email-mismatch c/kasittelytilat)]
    :sms_kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]
    :voimassaloppupvm [:s "2022-02-28"]}
   {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-6"]
    :niputuspvm [:s "2022-01-27"]
    :kyselylinkki [:s "kysely.linkki/6"]
    :kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]
    :sms_kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]
    :voimassaloppupvm [:s "2022-02-01"]}])

(defn- setup-test []
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mhc/bind-url :get
                (str (:organisaatio-url mock-env) "testilaitos")
                {:as :json}
                {:body {:nimi {:fi "Testilaitos"
                               :sv "Testanstalt"
                               :en "Test AMK"}}})
  (mhc/bind-url :post
                "https://viestipalvelu-api.elisa.fi/api/v1/"
                {:headers {:Authorization "apikey elisa-apikey"
                           :content-type "application/json"}
                 :body    (generate-string {:sender "OPH"
                                            :destination ["0401234567"]
                                            :text (elisa/msg-body
                                                    "kysely.linkki/1"
                                                    [{:fi "Testilaitos"
                                                      :sv "Testanstalt"
                                                      :en "Test AMK"}])})
                 :as      :json}
                {:body {:messages {:0401234567 {:converted "+358401234567"
                                                :status "CREATED"}}}})
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
                            "smsIndex"
                            {:primary-key :sms_kasittelytila
                             :sort-key :niputuspvm}))

(defn- teardown-test []
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mdb/clear-mock-db))

(def expected-jaksotunnus-table (into #{} starting-jaksotunnus-table))

(def expected-nippu-table
  #{{:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
     :niputuspvm [:s "2022-01-27"]
     :kyselylinkki [:s "kysely.linkki/1"]
     :kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]
     :sms_kasittelytila [:s "CREATED"]
     :voimassaloppupvm [:s "2022-03-04"]
     :lahetettynumeroon [:s "+358401234567"]
     :sms_lahetyspvm [:s "2022-02-02"]
     :sms_muistutukset [:n 0]}
    {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-2"]
     :niputuspvm [:s "2022-01-27"]
     :kyselylinkki [:s "kysely.linkki/2"]
     :kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]
     :sms_kasittelytila [:s (:phone-invalid c/kasittelytilat)]
     :voimassaloppupvm [:s "2022-02-28"]
     :lahetettynumeroon [:s "00000"]}
    {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-3"]
     :niputuspvm [:s "2022-01-27"]
     :kyselylinkki [:s "kysely.linkki/3"]
     :kasittelytila [:s (:no-email c/kasittelytilat)]
     :sms_kasittelytila [:s (:phone-invalid c/kasittelytilat)]
     :voimassaloppupvm [:s "2022-02-28"]
     :lahetettynumeroon [:s "00000"]}
    {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-4"]
     :niputuspvm [:s "2022-01-27"]
     :kyselylinkki [:s "kysely.linkki/4"]
     :kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]
     :sms_kasittelytila [:s (:no-phone c/kasittelytilat)]
     :voimassaloppupvm [:s "2022-02-28"]}
    {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-5"]
     :niputuspvm [:s "2022-01-27"]
     :kyselylinkki [:s "kysely.linkki/5"]
     :kasittelytila [:s (:email-mismatch c/kasittelytilat)]
     :sms_kasittelytila [:s (:phone-mismatch c/kasittelytilat)]
     :voimassaloppupvm [:s "2022-02-28"]}
    {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-6"]
     :niputuspvm [:s "2022-01-27"]
     :kyselylinkki [:s "kysely.linkki/6"]
     :kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]
     :sms_kasittelytila [:s (:vastausaika-loppunut c/kasittelytilat)]
     :voimassaloppupvm [:s "2022-02-01"]
     :sms_lahetyspvm [:s "2022-02-02"]}})

(def expected-http-results
  [{:method :get
    :url "testilaitos"
    :options {:as :json}}
   {:method :post
    :url "https://viestipalvelu-api.elisa.fi/api/v1/"
    :options {:headers {:Authorization "apikey elisa-apikey"
                        :content-type "application/json"}
              :body    (generate-string {:sender "OPH"
                                         :destination ["0401234567"]
                                         :text (elisa/msg-body
                                                 "kysely.linkki/1"
                                                 [{:fi "Testilaitos"
                                                   :sv "Testanstalt"
                                                   :en "Test AMK"}])})
              :as      :json}}
   {:method :patch
    :url (str (:arvo-url mock-env) "tyoelamapalaute/v1/nippu/1")
    :options {:basic-auth   [(:arvo-user mock-env) "arvo-pwd"]
              :content-type "application/json"
              :body         (str "{\"tila\":\"lahetetty\","
                                 "\"voimassa_loppupvm\":\"2022-03-04\"}")
              :as           :json}}
   {:method :get
    :url "testilaitos"
    :options {:as :json}}
   {:method :get
    :url "testilaitos"
    :options {:as :json}}
   {:method :patch
    :url (str (:arvo-url mock-env) "tyoelamapalaute/v1/nippu/3")
    :options {:basic-auth   ["arvo-user" "arvo-pwd"]
              :content-type "application/json"
              :body         "{\"tila\":\"ei_kelvollisia_yhteystietoja\"}"
              :as           :json}}
   {:method :get
    :url "testilaitos"
    :options {:as :json}}
   {:method :get
    :url "testilaitos"
    :options {:as :json}}
   {:method :patch
    :url (str (:arvo-url mock-env) "tyoelamapalaute/v1/nippu/5")
    :options {:basic-auth   ["arvo-user" "arvo-pwd"]
              :content-type "application/json"
              :body         "{\"tila\":\"ei_kelvollisia_yhteystietoja\"}"
              :as           :json}}])

(deftest test-tepSmsHandler-integration
  (testing "tepSmsHandler integraatiotesti"
    (with-redefs [environ.core/env mock-env
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 2 2))
                  oph.heratepalvelu.db.dynamodb/query-items mdb/query-items
                  oph.heratepalvelu.db.dynamodb/update-item mdb/update-item
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.elisa/apikey (delay "elisa-apikey")
                  oph.heratepalvelu.external.http-client/get mhc/mock-get
                  oph.heratepalvelu.external.http-client/patch mhc/mock-patch
                  oph.heratepalvelu.external.http-client/post mhc/mock-post]
      (setup-test)
      (tsh/-handleTepSmsSending {}
                                (tu/mock-handler-event :scheduledherate)
                                (tu/mock-handler-context))
      (is (= (mdb/get-table-values (:jaksotunnus-table mock-env))
             expected-jaksotunnus-table))
      (is (= (mdb/get-table-values (:nippu-table mock-env))
             expected-nippu-table))
      (is (= (mhc/get-results) expected-http-results))
      (teardown-test))))
