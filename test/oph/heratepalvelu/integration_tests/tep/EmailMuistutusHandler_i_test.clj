(ns oph.heratepalvelu.integration-tests.tep.EmailMuistutusHandler-i-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.integration-tests.mock-cas-client :as mcc]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.integration-tests.mock-http-client :as mhc]
            [oph.heratepalvelu.tep.EmailMuistutusHandler :as emh]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(def mock-env {:jaksotunnus-table "jaksotunnus-table-name"
               :nippu-table "nippu-table-name"
               :arvo-url "https://oph-arvo.com/"
               :arvo-user "arvo-user"
               :organisaatio-url "https://oph-organisaatio.com/"
               :viestintapalvelu-url "https://oph-viestintapalvelu.com"})

(def starting-jaksotunnus-table [{:hankkimistapa_id [:n 1]
                                  :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
                                  :niputuspvm [:s "2022-02-01"]
                                  :oppilaitos [:s "testilaitos"]}
                                 {:hankkimistapa_id [:n 2]
                                  :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-2"]
                                  :niputuspvm [:s "2022-02-01"]
                                  :oppilaitos [:s "testilaitos"]}
                                 {:hankkimistapa_id [:n 3]
                                  :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-3"]
                                  :niputuspvm [:s "2022-02-01"]
                                  :oppilaitos [:s "testilaitos"]}])

(def starting-nippu-table [{:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
                            :niputuspvm [:s "2022-02-01"]
                            :lahetysosoite [:s "ohjaaja1@esimerkki.fi"]
                            :kyselylinkki [:s "kysely.linkki/1"]
                            :muistutukset [:n 0]
                            :lahetyspvm [:s "2022-01-27"]}
                           {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-2"]
                            :niputuspvm [:s "2022-02-01"]
                            :lahetysosoite [:s "ohjaaja2@esimerkki.fi"]
                            :kyselylinkki [:s "kysely.linkki/2"]
                            :muistutukset [:n 0]
                            :lahetyspvm [:s "2022-01-27"]}
                           {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-3"]
                            :niputuspvm [:s "2022-02-01"]
                            :lahetysosoite [:s "ohjaaja3@esimerkki.fi"]
                            :kyselylinkki [:s "kysely.linkki/3"]
                            :muistutukset [:n 0]
                            :lahetyspvm [:s "2022-01-27"]}
                           {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-4"]
                            :niputuspvm [:s "2022-02-01"]
                            :lahetysosoite [:s "ohjaaja4@esimerkki.fi"]
                            :kyselylinkki [:s "kysely.linkki/4"]
                            :muistutukset [:n 1]
                            :lahetyspvm [:s "2022-01-27"]}
                           {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-5"]
                            :niputuspvm [:s "2022-02-01"]
                            :lahetysosoite [:s "ohjaaja5@esimerkki.fi"]
                            :kyselylinkki [:s "kysely.linkki/5"]
                            :muistutukset [:n 0]
                            :lahetyspvm [:s "2022-01-31"]}])

(defn- setup-test []
  (mcc/clear-results)
  (mcc/clear-url-bindings)
  (mcc/bind-url :post
                (:viestintapalvelu-url mock-env)
                {:recipient [{:email "ohjaaja1@esimerkki.fi"}]
                 :email {:callingProcess "heratepalvelu"
                         :from "no-reply@opintopolku.fi"
                         :sender "OPH – UBS – EDUFI"
                         :subject (str "Muistutus-påminnelse-reminder: "
                                       "Työpaikkaohjaajakysely - "
                                       "Enkät till arbetsplatshandledaren - "
                                       "Survey to workplace instructors")
                         :isHtml true
                         :body (vp/tyopaikkaohjaaja-muistutus-html
                                 {:kyselylinkki "kysely.linkki/1"}
                                 [{:fi "Testilaitos"
                                   :sv "Testanstalt"
                                   :en "Test School"}])}}
                {:body {:id 1}})
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mhc/bind-url :get
                (str (:arvo-url mock-env) "tyoelamapalaute/v1/status/1")
                {:basic-auth [(:arvo-user mock-env) "arvo-pwd"] :as :json}
                {:body {:vastattu false
                        :voimassa_loppupvm "2022-03-01"}})
  (mhc/bind-url :get
                (str (:arvo-url mock-env) "tyoelamapalaute/v1/status/2")
                {:basic-auth [(:arvo-user mock-env) "arvo-pwd"] :as :json}
                {:body {:vastattu true
                        :voimassa_loppupvm "2022-03-01"}})
  (mhc/bind-url :get
                (str (:arvo-url mock-env) "tyoelamapalaute/v1/status/3")
                {:basic-auth [(:arvo-user mock-env) "arvo-pwd"] :as :json}
                {:body {:vastattu false
                        :voimassa_loppupvm "2022-01-05"}})
  (mhc/bind-url :get
                (str (:organisaatio-url mock-env) "testilaitos")
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
                            "emailMuistutusIndex"
                            {:primary-key :muistutukset
                             :sort-key :lahetyspvm}))

(defn- teardown-test []
  (mcc/clear-results)
  (mcc/clear-url-bindings)
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mdb/clear-mock-db))

(def expected-jaksotunnus-table #{{:hankkimistapa_id [:n 1]
                                   :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
                                   :niputuspvm [:s "2022-02-01"]
                                   :oppilaitos [:s "testilaitos"]}
                                  {:hankkimistapa_id [:n 2]
                                   :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-2"]
                                   :niputuspvm [:s "2022-02-01"]
                                   :oppilaitos [:s "testilaitos"]}
                                  {:hankkimistapa_id [:n 3]
                                   :ohjaaja_ytunnus_kj_tutkinto [:s "oykt-3"]
                                   :niputuspvm [:s "2022-02-01"]
                                   :oppilaitos [:s "testilaitos"]}})

(def expected-nippu-table
  #{{:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
     :niputuspvm [:s "2022-02-01"]
     :lahetysosoite [:s "ohjaaja1@esimerkki.fi"]
     :kyselylinkki [:s "kysely.linkki/1"]
     :muistutukset [:n 1]
     :lahetyspvm [:s "2022-01-27"]
     :muistutus-viestintapalvelu-id [:n 1]
     :email_muistutuspvm [:s "2022-02-02"]}
    {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-2"]
     :niputuspvm [:s "2022-02-01"]
     :lahetysosoite [:s "ohjaaja2@esimerkki.fi"]
     :kyselylinkki [:s "kysely.linkki/2"]
     :muistutukset [:n 1]
     :lahetyspvm [:s "2022-01-27"]
     :kasittelytila [:s (:vastattu c/kasittelytilat)]}
    {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-3"]
     :niputuspvm [:s "2022-02-01"]
     :lahetysosoite [:s "ohjaaja3@esimerkki.fi"]
     :kyselylinkki [:s "kysely.linkki/3"]
     :muistutukset [:n 1]
     :lahetyspvm [:s "2022-01-27"]
     :kasittelytila [:s (:vastausaika-loppunut-m c/kasittelytilat)]}
    {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-4"]
     :niputuspvm [:s "2022-02-01"]
     :lahetysosoite [:s "ohjaaja4@esimerkki.fi"]
     :kyselylinkki [:s "kysely.linkki/4"]
     :muistutukset [:n 1]
     :lahetyspvm [:s "2022-01-27"]}
    {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-5"]
     :niputuspvm [:s "2022-02-01"]
     :lahetysosoite [:s "ohjaaja5@esimerkki.fi"]
     :kyselylinkki [:s "kysely.linkki/5"]
     :muistutukset [:n 0]
     :lahetyspvm [:s "2022-01-31"]}})

(def expected-http-results [{:method :get
                             :url (str (:arvo-url mock-env)
                                       "tyoelamapalaute/v1/status/1")
                             :options {:basic-auth ["arvo-user" "arvo-pwd"]
                                       :as :json}}
                            {:method :get
                             :url "https://oph-organisaatio.com/testilaitos"
                             :options {:as :json}}
                            {:method :get
                             :url (str (:arvo-url mock-env)
                                       "tyoelamapalaute/v1/status/2")
                             :options {:basic-auth ["arvo-user" "arvo-pwd"]
                                       :as :json}}
                            {:method :get
                             :url (str (:arvo-url mock-env)
                                       "tyoelamapalaute/v1/status/3")
                             :options {:basic-auth ["arvo-user" "arvo-pwd"]
                                       :as :json}}])

(def expected-cas-client-results
  [{:method :post
    :url (:viestintapalvelu-url mock-env)
    :body {:recipient [{:email "ohjaaja1@esimerkki.fi"}]
           :email {:callingProcess "heratepalvelu"
                   :from "no-reply@opintopolku.fi"
                   :sender "OPH – UBS – EDUFI"
                   :subject (str "Muistutus-påminnelse-reminder: "
                                 "Työpaikkaohjaajakysely - "
                                 "Enkät till arbetsplatshandledaren - "
                                 "Survey to workplace instructors")
                   :isHtml true
                   :body (vp/tyopaikkaohjaaja-muistutus-html
                           {:kyselylinkki "kysely.linkki/1"}
                           [{:fi "Testilaitos"
                             :sv "Testanstalt"
                             :en "Test School"}])}}
    :options {:as :json}}])

(deftest test-EmailMuistutusHandler-integration
  (testing "EmailMuistutusHandler integraatiotesti"
    (with-redefs [environ.core/env mock-env
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 2 2))
                  oph.heratepalvelu.db.dynamodb/query-items mdb/query-items
                  oph.heratepalvelu.db.dynamodb/update-item mdb/update-item
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.cas-client/cas-authenticated-post
                  mcc/mock-cas-authenticated-post
                  oph.heratepalvelu.external.http-client/get mhc/mock-get]
      (setup-test)
      (emh/-handleSendEmailMuistutus {}
                                     (tu/mock-handler-event :scheduledherate)
                                     (tu/mock-handler-context))
      (is (= (mdb/get-table-values (:jaksotunnus-table mock-env))
             expected-jaksotunnus-table))
      (is (= (mdb/get-table-values (:nippu-table mock-env))
             expected-nippu-table))
      (is (= (mcc/get-results) expected-cas-client-results))
      (is (= (mhc/get-results) expected-http-results))
      (teardown-test))))
