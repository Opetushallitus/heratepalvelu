(ns oph.heratepalvelu.integration-tests.tep.emailHandler-i-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.integration-tests.mock-cas-client :as mcc]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.integration-tests.mock-http-client :as mhc]
            [oph.heratepalvelu.tep.emailHandler :as eh]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(def mock-env {:jaksotunnus-table "jaksotunnus-table-name"
               :nippu-table "nippu-table-name"
               :arvo-url "https://oph-arvo.com/"
               :arvo-user "arvo-user"
               :organisaatio-url "https://oph-organisaatio.com/"
               :viestintapalvelu-url "https://oph-viestintapalvelu.com"})

(def starting-jaksotunnus-table [{:hankkimistapa_id [:n 11]
                                  :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
                                  :niputuspvm [:s "2022-01-05"]
                                  :ohjaaja_email [:s "ohjaaja11@esimerkki.fi"]
                                  :oppilaitos [:s "test-laitos"]}
                                 {:hankkimistapa_id [:n 21]
                                  :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-2"]
                                  :niputuspvm [:s "2022-01-05"]
                                  :ohjaaja_email [:s "ohjaaja21@esimerkki.fi"]
                                  :oppilaitos [:s "test-laitos"]}
                                 {:hankkimistapa_id [:n 22]
                                  :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-2"]
                                  :niputuspvm [:s "2022-01-05"]
                                  :ohjaaja_email [:s "ohjaaja22@esimerkki.fi"]
                                  :oppilaitos [:s "test-laitos"]}
                                 {:hankkimistapa_id [:n 31]
                                  :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-3"]
                                  :niputuspvm [:s "2022-01-05"]
                                  :ohjaaja_email [:s "ohjaaja3@esimerkki.fi"]
                                  :oppilaitos [:s "test-laitos"]}])

(def starting-nippu-table
  [{:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
    :niputuspvm [:s "2022-01-05"]
    :kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]
    :kyselylinkki [:s "kysely.linkki/1"]
    :sms_kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]
    :voimassaloppupvm [:s "2022-03-01"]}
   {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-2"]
    :niputuspvm [:s "2022-01-05"]
    :kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]
    :kyselylinkki [:s "kysely.linkki/2"]
    :sms_kasittelytila [:s (:no-phone c/kasittelytilat)]
    :voimassaloppupvm [:s "2022-03-01"]}
   {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-3"]
    :niputuspvm [:s "2022-01-05"]
    :kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]
    :kyselylinkki [:s "kysely.linkki/3"]
    :sms_kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]
    :voimassaloppupvm [:s "2022-01-01"]}])

(defn- setup-test []
  (mcc/clear-results)
  (mcc/clear-url-bindings)
  (mcc/bind-url :post
                (:viestintapalvelu-url mock-env)
                {:recipient [{:email "ohjaaja11@esimerkki.fi"}]
                 :email {:callingProcess "heratepalvelu"
                         :from "no-reply@opintopolku.fi"
                         :sender "OPH – UBS – EDUFI"
                         :subject (str "Työpaikkaohjaajakysely - "
                                       "Enkät till arbetsplatshandledaren - "
                                       "Survey to workplace instructors")
                         :isHtml true
                         :body (vp/tyopaikkaohjaaja-html
                                 {:kyselylinkki "kysely.linkki/1"}
                                 [{:fi "Testilaitos"
                                   :sv "Testanstalt"
                                   :en "Test School"}])}}
                {:body {:id 1}})
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mhc/bind-url :get
                (str (:organisaatio-url mock-env) "test-laitos")
                {:as :json}
                {:body {:nimi {:fi "Testilaitos"
                               :sv "Testanstalt"
                               :en "Test School"}}})
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
                                   :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
                                   :niputuspvm [:s "2022-01-05"]
                                   :ohjaaja_email [:s "ohjaaja11@esimerkki.fi"]
                                   :oppilaitos [:s "test-laitos"]}
                                  {:hankkimistapa_id [:n 21]
                                   :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-2"]
                                   :niputuspvm [:s "2022-01-05"]
                                   :ohjaaja_email [:s "ohjaaja21@esimerkki.fi"]
                                   :oppilaitos [:s "test-laitos"]}
                                  {:hankkimistapa_id [:n 22]
                                   :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-2"]
                                   :niputuspvm [:s "2022-01-05"]
                                   :ohjaaja_email [:s "ohjaaja22@esimerkki.fi"]
                                   :oppilaitos [:s "test-laitos"]}
                                  {:hankkimistapa_id [:n 31]
                                   :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-3"]
                                   :niputuspvm [:s "2022-01-05"]
                                   :ohjaaja_email [:s "ohjaaja3@esimerkki.fi"]
                                   :oppilaitos [:s "test-laitos"]}})

(def expected-nippu-table
  #{{:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
     :niputuspvm [:s "2022-01-05"]
     :lahetysosoite [:s "ohjaaja11@esimerkki.fi"]
     :kyselylinkki [:s "kysely.linkki/1"]
     :lahetyspvm [:s "2022-02-02"]
     :muistutukset [:n 0]
     :voimassaloppupvm [:s "2022-03-01"]
     :viestintapalvelu-id [:n 1]
     :sms_kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]
     :kasittelytila [:s (:viestintapalvelussa c/kasittelytilat)]}
    {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-2"]
     :niputuspvm [:s "2022-01-05"]
     :kasittelytila [:s (:email-mismatch c/kasittelytilat)]
     :kyselylinkki [:s "kysely.linkki/2"]
     :sms_kasittelytila [:s (:no-phone c/kasittelytilat)]
     :voimassaloppupvm [:s "2022-03-01"]}
    {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-3"]
     :niputuspvm [:s "2022-01-05"]
     :kasittelytila [:s (:vastausaika-loppunut c/kasittelytilat)]
     :kyselylinkki [:s "kysely.linkki/3"]
     :sms_kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]
     :voimassaloppupvm [:s "2022-01-01"]
     :lahetyspvm [:s "2022-02-02"]}})

(def expected-http-results [{:method :get
                             :url "https://oph-organisaatio.com/test-laitos"
                             :options {:as :json}}
                            {:method :get
                             :url "https://oph-organisaatio.com/test-laitos"
                             :options {:as :json}}
                            {:method :get
                             :url "https://oph-organisaatio.com/test-laitos"
                             :options {:as :json}}
                            {:method :patch
                             :url (str (:arvo-url mock-env)
                                       "tyoelamapalaute/v1/nippu/2")
                             :options
                             {:basic-auth ["arvo-user" "arvo-pwd"]
                              :content-type "application/json"
                              :body
                              (str "{\"tila\":\"ei_kelvollisia_yhteystietoja\","
                                   "\"voimassa_loppupvm\":\"2022-03-01\"}")
                              :as :json}}
                            {:method :get
                             :url "https://oph-organisaatio.com/test-laitos"
                             :options {:as :json}}])

(def expected-cas-client-results
  [{:method :post
    :url (:viestintapalvelu-url mock-env)
    :body {:recipient [{:email "ohjaaja11@esimerkki.fi"}]
           :email {:callingProcess "heratepalvelu"
                   :from "no-reply@opintopolku.fi"
                   :sender "OPH – UBS – EDUFI"
                   :subject (str "Työpaikkaohjaajakysely - "
                                 "Enkät till arbetsplatshandledaren - "
                                 "Survey to workplace instructors")
                   :isHtml true
                   :body (vp/tyopaikkaohjaaja-html
                           {:kyselylinkki "kysely.linkki/1"}
                           [{:fi "Testilaitos"
                             :sv "Testanstalt"
                             :en "Test School"}])}}
    :options {:as :json}}])

(deftest test-emailHandler-integration
  (testing "emailHandler integraatiotesti"
    (with-redefs [environ.core/env mock-env
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 2 2))
                  oph.heratepalvelu.db.dynamodb/query-items mdb/query-items
                  oph.heratepalvelu.db.dynamodb/update-item mdb/update-item
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.cas-client/cas-authenticated-post
                  mcc/mock-cas-authenticated-post
                  oph.heratepalvelu.external.http-client/get mhc/mock-get
                  oph.heratepalvelu.external.http-client/patch mhc/mock-patch]
      (setup-test)
      (eh/-handleSendTEPEmails {}
                               (tu/mock-handler-event :scheduledherate)
                               (tu/mock-handler-context))
      (is (= (mdb/get-table-values (:jaksotunnus-table mock-env))
             expected-jaksotunnus-table))
      (is (= (mdb/get-table-values (:nippu-table mock-env))
             expected-nippu-table))
      (is (= (mhc/get-results) expected-http-results))
      (is (= (mcc/get-results) expected-cas-client-results))
      (teardown-test))))
