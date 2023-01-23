(ns oph.heratepalvelu.integration-tests.tep.ehoksTimedOperationsHandler-i-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.integration-tests.mock-cas-client :as mcc]
            [oph.heratepalvelu.integration-tests.mock-http-client :as mhc]
            [oph.heratepalvelu.tep.ehoksTimedOperationsHandler :as etoh]
            [oph.heratepalvelu.test-util :as tu]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb])
  (:import (java.time LocalDate)))

(def mock-env {:ehoks-url "https://oph-ehoks.com/"
               :jaksotunnus-table "jaksotunnus-table"})

(def starting-table-contents [{:hankkimistapa_id [:n 1]
                               :ohjaaja_email [:s "ohjaaja1@yritys.fi"]
                               :ohjaaja_puhelinnumero [:s "0401111111"]}
                              {:hankkimistapa_id [:n 2]
                               :ohjaaja_email [:s "ohjaaja2@yritys.fi"]
                               :ohjaaja_puhelinnumero [:s "0401111112"]}
                              {:hankkimistapa_id [:n 3]
                               :ohjaaja_email [:s "ohjaaja3@yritys.fi"]
                               :ohjaaja_puhelinnumero [:s "0401111113"]}])

(defn- setup-test []
  (mcc/clear-results)
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mhc/bind-url :get
                (str (:ehoks-url mock-env) "heratepalvelu/tyoelamajaksot")
                {:query-params {:start "2021-07-01"
                                :end "2022-02-02"
                                :limit 1500}
                 :as :json
                 :headers
                 {:ticket
                  "service-ticket/ehoks-virkailija-backend/cas-security-check"}}
                {:body {:data 2}})
  (mhc/bind-url :delete
                (str (:ehoks-url mock-env)
                     "heratepalvelu/tyopaikkaohjaajan-yhteystiedot")
                {:as :json
                 :headers
                 {:ticket
                  "service-ticket/ehoks-virkailija-backend/cas-security-check"}}
                {:body {:data {:hankkimistapa-ids [1 2 3]}}})
  (mdb/clear-mock-db)
  (mdb/create-table (:jaksotunnus-table mock-env)
                    {:primary-key :hankkimistapa_id
                     :sort-key :hankkimistapa_id})
  (mdb/set-table-contents (:jaksotunnus-table mock-env) starting-table-contents))

(defn- teardown-test []
  (mcc/clear-results)
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mdb/clear-mock-db))

(def expected-http-results
  [{:method :get
    :url "https://oph-ehoks.com/heratepalvelu/tyoelamajaksot"
    :options
    {:headers {:ticket
               "service-ticket/ehoks-virkailija-backend/cas-security-check"}
     :as :json
     :query-params {:start "2021-07-01" :end "2022-02-02" :limit 1500}}}
   {:method :delete
    :url "https://oph-ehoks.com/heratepalvelu/tyopaikkaohjaajan-yhteystiedot"
    :options
    {:headers {:ticket
               "service-ticket/ehoks-virkailija-backend/cas-security-check"}
     :as :json}}])

(def expected-cas-client-results [{:type :get-service-ticket
                                   :service "/ehoks-virkailija-backend"
                                   :suffix "cas-security-check"}
                                  {:type :get-service-ticket
                                   :service "/ehoks-virkailija-backend"
                                   :suffix "cas-security-check"}])

(deftest test-ehoksTimedOperationsHandler-integration
  (testing "ehoksTimedIntegrationsHandler integraatiotesti"
    (with-redefs [clojure.tools.logging/log* tu/mock-log*
                  environ.core/env mock-env
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 2 2))
                  oph.heratepalvelu.external.cas-client/get-service-ticket
                  mcc/mock-get-service-ticket
                  oph.heratepalvelu.external.http-client/get mhc/mock-get
                  oph.heratepalvelu.external.http-client/delete mhc/mock-delete
                  ddb/scan mdb/scan
                  ddb/update-item mdb/update-item]
      (setup-test)
      (etoh/-handleTimedOperations {}
                                   (tu/mock-handler-event :scheduledherate)
                                   (tu/mock-handler-context))
      (is (= (mcc/get-results) expected-cas-client-results))
      (is (= (mhc/get-results) expected-http-results))
      (is (true? (tu/logs-contain?
                   {:level :info
                    :message "Käynnistetään jaksojen lähetys"})))
      (is (true? (tu/logs-contain? {:level :info
                                    :message "Lähetetty 2 viestiä"})))

      (is (true? (tu/logs-contain?
                   {:level :info
                    :message
                    "Käynnistetään työpaikkaohjaajan yhteystietojen poisto"})))
      (is (true? (tu/logs-contain?
                   {:level :info
                    :message "Poistettu 3 ohjaajan yhteystiedot"})))
      (is (= (mdb/get-table-values (:jaksotunnus-table mock-env))
             #{{:hankkimistapa_id [:n 1]
                :ohjaaja_email [:s ""]
                :ohjaaja_puhelinnumero [:s ""]}
               {:hankkimistapa_id [:n 2]
                :ohjaaja_email [:s ""]
                :ohjaaja_puhelinnumero [:s ""]}
               {:hankkimistapa_id [:n 3]
                :ohjaaja_email [:s ""]
                :ohjaaja_puhelinnumero [:s ""]}}))

      (teardown-test))))
