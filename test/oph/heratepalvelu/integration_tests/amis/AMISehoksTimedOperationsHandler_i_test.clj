(ns oph.heratepalvelu.integration-tests.amis.AMISehoksTimedOperationsHandler-i-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.amis.AMISehoksTimedOperationsHandler :as toh]
            [oph.heratepalvelu.integration-tests.mock-cas-client :as mcc]
            [oph.heratepalvelu.integration-tests.mock-http-client :as mhc]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(def mock-env {:ehoks-url "https://oph-ehoks.com/"})

(defn- setup-test []
  (mcc/clear-results)
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mhc/bind-url
    :get
    (str (:ehoks-url mock-env) "heratepalvelu/kasittelemattomat-heratteet")
    {:query-params {:start "2021-07-01"
                    :end "2022-02-02"
                    :limit 1000}
     :as :json
     :headers {:ticket
               "service-ticket/ehoks-virkailija-backend/cas-security-check"}}
    {:body {:data 2}}))

(defn- teardown-test []
  (mcc/clear-results)
  (mhc/clear-results)
  (mhc/clear-url-bindings))

(def expected-http-results
  [{:method :get
    :url "https://oph-ehoks.com/heratepalvelu/kasittelemattomat-heratteet"
    :options
    {:headers {:ticket
               "service-ticket/ehoks-virkailija-backend/cas-security-check"}
     :query-params {:start "2021-07-01"
                    :end "2022-02-02"
                    :limit 1000}
     :as :json}}])

(def expected-cas-client-results [{:type :get-service-ticket
                                   :service "/ehoks-virkailija-backend"
                                   :suffix "cas-security-check"}])

(deftest test-AMISehoksTimedOperationsHandler-integration
  (testing "AMISehoksTimedOperationsHandler integraatiotesti"
    (with-redefs [clojure.tools.logging/log* tu/mock-log*
                  environ.core/env mock-env
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 2 2))
                  oph.heratepalvelu.external.cas-client/get-service-ticket
                  mcc/mock-get-service-ticket
                  oph.heratepalvelu.external.http-client/get mhc/mock-get]
      (setup-test)
      (toh/-handleAMISTimedOperations {}
                                      (tu/mock-handler-event :scheduledherate)
                                      (tu/mock-handler-context))
      (is (= (mcc/get-results) expected-cas-client-results))
      (is (= (mhc/get-results) expected-http-results))
      (is (true? (tu/logs-contain?
                   {:level :info
                    :message "Käynnistetään herätteiden lähetys"})))
      (is (true? (tu/logs-contain? {:level :info
                                    :message "Lähetetty 2 viestiä"})))
      (teardown-test))))


;; -handleMassHerateResend testi

(defn- setup-mass-test []
  (mcc/clear-results)
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mhc/bind-url
    :post
    (str (:ehoks-url mock-env) "heratepalvelu/hoksit/resend-aloitusherate")
    {:query-params {:from "2022-01-19" :to "2022-02-02"}
     :as :json
     :headers {:ticket
               "service-ticket/ehoks-virkailija-backend/cas-security-check"}}
    {:body {:data {:count 2}}})
  (mhc/bind-url
    :post
    (str (:ehoks-url mock-env) "heratepalvelu/hoksit/resend-paattoherate")
    {:query-params {:from "2022-01-19" :to "2022-02-02"}
     :as :json
     :headers {:ticket
               "service-ticket/ehoks-virkailija-backend/cas-security-check"}}
    {:body {:data {:count 3}}}))

(defn- teardown-mass-test []
  (mcc/clear-results)
  (mhc/clear-results)
  (mhc/clear-url-bindings))

(def expected-mass-http-results
  [{:method :post
    :url "https://oph-ehoks.com/heratepalvelu/hoksit/resend-aloitusherate"
    :options
    {:headers {:ticket
               "service-ticket/ehoks-virkailija-backend/cas-security-check"}
     :query-params {:from "2022-01-19" :to "2022-02-02"}
     :as :json}}
   {:method :post
    :url "https://oph-ehoks.com/heratepalvelu/hoksit/resend-paattoherate"
    :options
    {:headers {:ticket
               "service-ticket/ehoks-virkailija-backend/cas-security-check"}
     :query-params {:from "2022-01-19" :to "2022-02-02"}
     :as :json}}])

(def expected-mass-cas-client-results [{:type :get-service-ticket
                                        :service "/ehoks-virkailija-backend"
                                        :suffix "cas-security-check"}
                                       {:type :get-service-ticket
                                        :service "/ehoks-virkailija-backend"
                                        :suffix "cas-security-check"}])

(deftest test-AMISMassHerateResend-integration
  (testing "MassHerateResend integraatiotesti"
    (with-redefs [clojure.tools.logging/log* tu/mock-log*
                  environ.core/env mock-env
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 2 2))
                  oph.heratepalvelu.external.cas-client/get-service-ticket
                  mcc/mock-get-service-ticket
                  oph.heratepalvelu.external.http-client/post mhc/mock-post]
      (setup-mass-test)
      (toh/-handleMassHerateResend {}
                                   (tu/mock-handler-event :scheduledherate)
                                   (tu/mock-handler-context))
      (is (= (mcc/get-results) expected-mass-cas-client-results))
      (is (= (mhc/get-results) expected-mass-http-results))
      (is (true?
            (tu/logs-contain?
              {:level :info
               :message "Käynnistetään herätteiden massauudelleenlähetys"})))
      (is (true? (tu/logs-contain?
                   {:level :info
                    :message "Lähetetty 2 aloitusviestiä ja 3 päättöviestiä"})))
      (teardown-mass-test))))
