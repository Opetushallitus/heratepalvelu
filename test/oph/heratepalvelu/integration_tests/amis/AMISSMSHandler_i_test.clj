(ns oph.heratepalvelu.integration-tests.amis.AMISSMSHandler-i-test
  (:require [cheshire.core :refer [generate-string]]
            [clojure.test :refer :all]
            [oph.heratepalvelu.amis.AMISSMSHandler :as ash]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.elisa :as elisa]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.integration-tests.mock-http-client :as mhc]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(def mock-env {:herate-table "herate-table-name"
               :organisaatio-url "https://oph-organisaatio.com/"
               :send-messages "true"})

(defn- mock-get-organisaatio [oppilaitos]
  {:nimi {:fi "Testilaitos" :en "Test Dept." :sv "Testanstalt"}})

(def starting-table-contents
  [{:toimija_oppija    [:s "abc/1"]
    :oppilaitos        [:s "test-laitos"]
    :tyyppi_kausi      [:s "aloittaneet/2021-2022"]
    :sms-lahetystila   [:s (:ei-laheteta c/kasittelytilat)]
    :alkupvm           [:s "2022-02-02"]
    :kyselylinkki      [:s "kysely.linkki/1"]}
   {:toimija_oppija    [:s "abc/2"]
    :oppilaitos        [:s "test-laitos"]
    :tyyppi_kausi      [:s "tutkinnon_suorittaneet/2021-2022"]
    :sms-lahetystila   [:s (:ei-lahetetty c/kasittelytilat)]
    :alkupvm           [:s "2022-02-02"]
    :kyselylinkki      [:s "kysely.linkki/2"]
    :voimassa-loppupvm [:s "2022-03-01"]}
   {:toimija_oppija    [:s "abc/3"]
    :oppilaitos        [:s "test-laitos"]
    :tyyppi_kausi      [:s "tutkinnon_suorittaneet/2021-2022"]
    :sms-lahetystila   [:s (:ei-lahetetty c/kasittelytilat)]
    :alkupvm           [:s "2022-02-02"]
    :puhelinnumero     [:s "asdfadsfafds"]
    :kyselylinkki      [:s "kysely.linkki/3"]
    :voimassa-loppupvm [:s "2022-04-04"]}
   {:toimija_oppija    [:s "abc/4"]
    :oppilaitos        [:s "test-laitos"]
    :tyyppi_kausi      [:s "tutkinnon_osia_suorittaneet/2021-2022"]
    :sms-lahetystila   [:s (:ei-lahetetty c/kasittelytilat)]
    :alkupvm           [:s "2022-02-02"]
    :puhelinnumero     [:s "12345"]
    :kyselylinkki      [:s "kysely.linkki/4"]
    :voimassa-loppupvm [:s "2022-04-04"]}])

(defn- setup-test []
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mhc/bind-url :post
                "https://viestipalvelu-api.elisa.fi/api/v1/"
                {:headers {:Authorization "apikey elisa-apikey"
                           :content-type "application/json"}
                 ;; TODO korjaa testi mockaamaan oppilaitoksen haku
                 :body    (generate-string {:sender "OPH"
                                            :destination ["12345"]
                                            :text (elisa/amis-msg-body
                                                    "kysely.linkki/4"
                                                    "Testilaitos")})
                 :as      :json}
                {:body {:messages {:12345 {:converted "+358 12345"
                                           :status "CREATED"}}}})
  (mhc/bind-url :get
                (str (:organisaatio-url mock-env) "test-laitos")
                {:as :json}
                {:body {:nimi {:fi "Testilaitos"
                               :sv "Testanstalt"
                               :en "Test School"}}})

  (mdb/create-table (:herate-table mock-env) {:primary-key :toimija_oppija
                                              :sort-key    :tyyppi_kausi})
  (mdb/set-table-contents (:herate-table mock-env) starting-table-contents)
  (mdb/add-index-key-fields (:herate-table mock-env)
                            "smsIndex"
                            {:primary-key :sms-lahetystila
                             :sort-key    :alkupvm}))

(defn- teardown-test []
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mdb/clear-mock-db))

(def expected-table
  #{{:toimija_oppija    [:s "abc/1"]
     :oppilaitos        [:s "test-laitos"]
     :tyyppi_kausi      [:s "aloittaneet/2021-2022"]
     :sms-lahetystila   [:s (:ei-laheteta c/kasittelytilat)]
     :alkupvm           [:s "2022-02-02"]
     :kyselylinkki      [:s "kysely.linkki/1"]}
    {:toimija_oppija    [:s "abc/2"]
     :oppilaitos        [:s "test-laitos"]
     :tyyppi_kausi      [:s "tutkinnon_suorittaneet/2021-2022"]
     :sms-lahetystila   [:s (:vastausaika-loppunut c/kasittelytilat)]
     :sms-lahetyspvm    [:s "2022-03-03"]
     :alkupvm           [:s "2022-02-02"]
     :kyselylinkki      [:s "kysely.linkki/2"]
     :voimassa-loppupvm [:s "2022-03-01"]}
    {:toimija_oppija    [:s "abc/3"]
     :oppilaitos        [:s "test-laitos"]
     :tyyppi_kausi      [:s "tutkinnon_suorittaneet/2021-2022"]
     :sms-lahetystila   [:s (:phone-invalid c/kasittelytilat)]
     :sms-lahetyspvm    [:s "2022-03-03"]
     :alkupvm           [:s "2022-02-02"]
     :puhelinnumero     [:s "asdfadsfafds"]
     :kyselylinkki      [:s "kysely.linkki/3"]
     :voimassa-loppupvm [:s "2022-04-04"]}
    {:toimija_oppija    [:s "abc/4"]
     :oppilaitos        [:s "test-laitos"]
     :tyyppi_kausi      [:s "tutkinnon_osia_suorittaneet/2021-2022"]
     :sms-lahetystila   [:s "CREATED"]
     :sms-lahetyspvm    [:s "2022-03-03"]
     :lahetettynumeroon [:s "+358 12345"]
     :alkupvm           [:s "2022-02-02"]
     :puhelinnumero     [:s "12345"]
     :kyselylinkki      [:s "kysely.linkki/4"]
     :voimassa-loppupvm [:s "2022-04-04"]}})

(def expected-http-results
  [{:method :post
    :url "https://viestipalvelu-api.elisa.fi/api/v1/"
    :options
    {:headers {:Authorization "apikey elisa-apikey"
               :content-type "application/json"}
     :body    (generate-string {:sender "OPH"
                                :destination ["12345"]
                                :text (elisa/amis-msg-body "kysely.linkki/4" "Testilaitos")})
     :as      :json}}])

(deftest test-AMISSMSHerate-integration
  (testing "AMISSMSHandler integraatiotesti"
    (with-redefs [environ.core/env mock-env
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 3 3))
                  oph.heratepalvelu.common/valid-number? #(= % "12345")
                  oph.heratepalvelu.db.dynamodb/query-items mdb/query-items
                  oph.heratepalvelu.db.dynamodb/update-item mdb/update-item
                  oph.heratepalvelu.external.elisa/apikey (delay "elisa-apikey")
                  oph.heratepalvelu.external.organisaatio/get-organisaatio
                  mock-get-organisaatio
                  oph.heratepalvelu.external.http-client/post mhc/mock-post]
      (setup-test)
      (ash/-handleAMISSMS {}
                          (tu/mock-handler-event :scheduledherate)
                          (tu/mock-handler-context))
      (is (= (mdb/get-table-values (:herate-table mock-env)) expected-table))
      (is (= (mhc/get-results) expected-http-results))
      (teardown-test))))
