(ns oph.heratepalvelu.external.ehoks-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.external.ehoks :as ehoks]))

(defn- mock-general-client-call [method uri options]
  {:body {:data {:type "mock-client-call-response"
                 :params {:method method :uri uri :options options}}}})

(defn- mock-client-get [uri options]
  (mock-general-client-call "get" uri options))

(defn- mock-get-service-ticket [service suffix]
  {:type "cas-service-ticket" :service service :suffix suffix})

(deftest test-get-hoks-by-opiskeluoikeus
  (testing "Varmista, että get-hoks-by-opiskeluoikeus toimii oikein"
    (with-redefs [environ.core/env {:ehoks-url "example.com/path/"}
                  oph.heratepalvelu.external.cas-client/get-service-ticket
                  mock-get-service-ticket
                  oph.heratepalvelu.external.http-client/get mock-client-get]
      (let [expected {:type "mock-client-call-response"
                      :params {:method "get"
                               :uri "example.com/path/hoks/opiskeluoikeus/1.2.3"
                               :options
                               {:headers {:ticket
                                          {:type "cas-service-ticket"
                                           :service "/ehoks-virkailija-backend"
                                           :suffix "cas-security-check"}}
                                :as :json}}}]
        (is (= (ehoks/get-hoks-by-opiskeluoikeus "1.2.3") expected))))))
