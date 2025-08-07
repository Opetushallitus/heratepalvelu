(ns oph.heratepalvelu.integration-tests.tep.SMSMuistutusHandler-i-test
  (:require [cheshire.core :refer [generate-string]]
            [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.elisa :as elisa]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.integration-tests.mock-http-client :as mhc]
            [oph.heratepalvelu.tep.SMSMuistutusHandler :as smh]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(def mock-env {:jaksotunnus-table "jaksotunnus-table-name"
               :nippu-table "nippu-table-name"
               :arvo-url "https://oph-arvo.com/"
               :arvo-user "arvo-user"
               :organisaatio-url "https://oph-organisaatio.com/"
               :send-messages "true"})

(def starting-jaksotunnus-table [{:hankkimistapa_id [:n 11]
                                  :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
                                  :niputuspvm [:s "2022-01-16"]
                                  :oppilaitos [:s "testilaitos"]}])

(def starting-nippu-table [{:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
                            :niputuspvm [:s "2022-01-16"]
                            :kyselylinkki [:s "kysely.linkki/AAAAAA"]
                            :lahetettynumeroon [:s "+358401234567"]
                            :sms_muistutukset [:n 0]
                            :sms_kasittelytila [:s (:success c/kasittelytilat)]
                            :sms_lahetyspvm [:s "2022-01-27"]}
                           {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-2"]
                            :niputuspvm [:s "2022-01-16"]
                            :kyselylinkki [:s "kysely.linkki/BBBBBB"]
                            :lahetettynumeroon [:s "+358401234567"]
                            :sms_muistutukset [:n 0]
                            :sms_kasittelytila [:s (:success c/kasittelytilat)]
                            :sms_lahetyspvm [:s "2022-01-27"]}
                           {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-3"]
                            :niputuspvm [:s "2022-01-16"]
                            :kyselylinkki [:s "kysely.linkki/CCCCCC"]
                            :lahetettynumeroon [:s "+358401234567"]
                            :sms_muistutukset [:n 0]
                            :sms_kasittelytila [:s (:success c/kasittelytilat)]
                            :sms_lahetyspvm [:s "2022-01-27"]}])

(defn- setup-test []
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mhc/bind-url :get
                (str (:arvo-url mock-env) "tyoelamapalaute/v1/status/AAAAAA")
                {:basic-auth [(:arvo-user mock-env) "arvo-pwd"] :as :json}
                {:body {:vastattu false
                        :voimassa_loppupvm "2022-03-31"}})
  (mhc/bind-url :get
                (str (:arvo-url mock-env) "tyoelamapalaute/v1/status/BBBBBB")
                {:basic-auth [(:arvo-user mock-env) "arvo-pwd"] :as :json}
                {:body {:vastattu false
                        :voimassa_loppupvm "2022-01-01"}})
  (mhc/bind-url :get
                (str (:arvo-url mock-env) "tyoelamapalaute/v1/status/CCCCCC")
                {:basic-auth [(:arvo-user mock-env) "arvo-pwd"] :as :json}
                {:body {:vastattu true
                        :voimassa_loppupvm "2022-03-31"}})
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
                 :body (generate-string
                         {:sender "OPH"
                          :destination ["+358401234567"]
                          :text (elisa/tep-muistutus-msg-body
                                  "kysely.linkki/AAAAAA"
                                  [{:fi "Testilaitos"
                                    :sv "Testanstalt"
                                    :en "Test AMK"}])})
                 :as :json}
                {:body {:messages
                        {:+358401234567
                         {:status (:viestintapalvelussa c/kasittelytilat)}}}})
  (mdb/clear-mock-db)
  (mdb/create-table (:nippu-table mock-env)
                    {:primary-key :ohjaaja_ytunnus_kj_tutkinto
                     :sort-key :niputuspvm})
  (mdb/set-table-contents (:nippu-table mock-env) starting-nippu-table)
  (mdb/add-index-key-fields (:nippu-table mock-env)
                            "smsMuistutusIndex"
                            {:primary-key :sms_muistutukset
                             :sort-key :sms_lahetyspvm})
  (mdb/create-table (:jaksotunnus-table mock-env)
                    {:primary-key :hankkimistapa_id})
  (mdb/set-table-contents (:jaksotunnus-table mock-env)
                          starting-jaksotunnus-table)
  (mdb/add-index-key-fields (:jaksotunnus-table mock-env)
                            "niputusIndex"
                            {:primary-key :ohjaaja_ytunnus_kj_tutkinto
                             :sort-key :niputuspvm}))

(defn- teardown-test []
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mdb/clear-mock-db))

(def expected-jaksotunnus-table (into #{} starting-jaksotunnus-table))

(def expected-nippu-table
  #{{:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
     :niputuspvm [:s "2022-01-16"]
     :kyselylinkki [:s "kysely.linkki/AAAAAA"]
     :lahetettynumeroon [:s "+358401234567"]
     :sms_kasittelytila [:s (:success c/kasittelytilat)]
     :sms_muistutukset [:n 1]
     :sms_muistutus_kasittelytila [:s (:viestintapalvelussa c/kasittelytilat)]
     :sms_lahetyspvm [:s "2022-01-27"]
     :sms_muistutuspvm [:s "2022-02-02"]}
    {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-2"]
     :niputuspvm [:s "2022-01-16"]
     :kyselylinkki [:s "kysely.linkki/BBBBBB"]
     :lahetettynumeroon [:s "+358401234567"]
     :sms_muistutukset [:n 1]
     :sms_kasittelytila [:s (:vastausaika-loppunut-m c/kasittelytilat)]
     :sms_lahetyspvm [:s "2022-01-27"]}
    {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-3"]
     :niputuspvm [:s "2022-01-16"]
     :kyselylinkki [:s "kysely.linkki/CCCCCC"]
     :lahetettynumeroon [:s "+358401234567"]
     :sms_muistutukset [:n 1]
     :sms_kasittelytila [:s (:vastattu c/kasittelytilat)]
     :sms_lahetyspvm [:s "2022-01-27"]}})

(def expected-http-results
  [{:method :get
    :url (str (:arvo-url mock-env) "tyoelamapalaute/v1/status/AAAAAA")
    :options {:basic-auth ["arvo-user" "arvo-pwd"]
              :as :json}}
   {:method :get
    :url (str (:organisaatio-url mock-env)
              "testilaitos")
    :options {:as :json}}
   {:method :post
    :url "https://viestipalvelu-api.elisa.fi/api/v1/"
    :options {:headers
              {:Authorization "apikey elisa-apikey"
               :content-type "application/json"}
              :body (generate-string
                      {:sender "OPH"
                       :destination ["+358401234567"]
                       :text (elisa/tep-muistutus-msg-body
                               "kysely.linkki/AAAAAA"
                               [{:fi "Testilaitos"
                                 :sv "Testanstalt"
                                 :en "Test AMK"}])})
              :as :json}}
   {:method :get
    :url (str (:arvo-url mock-env)
              "tyoelamapalaute/v1/status/BBBBBB")
    :options {:basic-auth ["arvo-user" "arvo-pwd"]
              :as :json}}
   {:method :get
    :url (str (:arvo-url mock-env)
              "tyoelamapalaute/v1/status/CCCCCC")
    :options {:basic-auth ["arvo-user" "arvo-pwd"]
              :as :json}}])

(deftest test-SMSMuistutusHandler-integration
  (testing "SMSMuistutusHandler integraatiotesti"
    (with-redefs [environ.core/env mock-env
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 2 2))
                  oph.heratepalvelu.db.dynamodb/query-items mdb/query-items
                  oph.heratepalvelu.db.dynamodb/update-item mdb/update-item
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.elisa/apikey (delay "elisa-apikey")
                  oph.heratepalvelu.external.http-client/get mhc/mock-get
                  oph.heratepalvelu.external.http-client/post mhc/mock-post]
      (setup-test)
      (smh/-handleSendSMSMuistutus {}
                                   (tu/mock-handler-event :scheduledherate)
                                   (tu/mock-handler-context))
      (is (= (mdb/get-table-values (:jaksotunnus-table mock-env))
             expected-jaksotunnus-table))
      (is (= (mdb/get-table-values (:nippu-table mock-env))
             expected-nippu-table))
      (is (= (mhc/get-results) expected-http-results))
      (teardown-test))))
