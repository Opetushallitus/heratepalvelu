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
    (with-redefs [oph.heratepalvelu.external.cas-client/get-service-ticket
                  mock-get-service-ticket
                  oph.heratepalvelu.external.http-client/get mock-client-get
