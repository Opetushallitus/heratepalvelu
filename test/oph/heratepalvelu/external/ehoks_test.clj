(ns oph.heratepalvelu.external.ehoks-test
  (:require [cheshire.core :refer [generate-string]]
            [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.ehoks :as ehoks]))

(defn- mock-general-client-call [method uri options]
  {:body {:data {:type "mock-client-call-response"
                 :params {:method method :uri uri :options options}}}})

(defn- mock-client-get [uri options]
  (mock-general-client-call "get" uri options))

(defn- mock-client-patch [uri options]
  (mock-general-client-call "patch" uri options))

(defn- mock-client-post [uri options]
  (mock-general-client-call "post" uri options))

(def has-errored? (atom false))

(defn- mock-client-patch-with-error [uri options]
  (if has-errored?
    (mock-general-client-call "patch" uri options)
    (throw (ex-info "some error" {:type "random-error"}))))

(defn- mock-client-post-with-error [uri options]
  (if has-errored?
    (mock-general-client-call "post" uri options)
    (throw (ex-info "some error" {:type "random-error"}))))

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

(deftest test-add-kyselytunnus-to-hoks
  (testing "Varmista, että add-kyselytunnus-to-hoks toimii oikein"
    (with-redefs [environ.core/env {:ehoks-url "example.com/path/"}
                  oph.heratepalvelu.external.cas-client/get-service-ticket
                  mock-get-service-ticket]
      (let [data {:kyselylinkki "kysely.linkki/asdf"
                  :tyyppi "aloittaneet"
                  :alkupvm "2021-12-15"
                  :lahetystila (:ei-lahetetty c/kasittelytilat)}
            expected {:body
                      {:data
                       {:type "mock-client-call-response"
                        :params
                        {:method "post"
                         :uri "example.com/path/hoks/1234/kyselylinkki"
                         :options
                         {:headers {:ticket
                                    {:type "cas-service-ticket"
                                     :service "/ehoks-virkailija-backend"
                                     :suffix "cas-security-check"}}
                          :content-type "application/json"
                          :body (generate-string data)
                          :as :json}}}}}]
        (with-redefs [oph.heratepalvelu.external.http-client/post
                      mock-client-post]
          (is (= (ehoks/add-kyselytunnus-to-hoks 1234 data) expected)))
        (with-redefs [oph.heratepalvelu.external.http-client/post
                      mock-client-post-with-error]
          (is (= (ehoks/add-kyselytunnus-to-hoks 1234 data) expected)))
        (reset! has-errored? false)))))

(deftest test-get-osaamisen-hankkimistapa-by-id
  (testing "Varmista, että get-osaamisen-hankkimistapa-by-id toimii oikein"
    (with-redefs [environ.core/env {:ehoks-url "example.com/path/"}
                  oph.heratepalvelu.external.cas-client/get-service-ticket
                  mock-get-service-ticket
                  oph.heratepalvelu.external.http-client/get mock-client-get]
      (let [expected {:type "mock-client-call-response"
                      :params
                      {:method "get"
                       :uri "example.com/path/hoks/osaamisen-hankkimistapa/123"
                       :options {:headers {:ticket
                                           {:type "cas-service-ticket"
                                            :service "/ehoks-virkailija-backend"
                                            :suffix "cas-security-check"}}
                                 :as :json}}}]
        (is (= (ehoks/get-osaamisen-hankkimistapa-by-id 123) expected))))))

(deftest test-get-hankintakoulutus-oids
  (testing "Varmista, että get-hankintakoulutus-oids toimii oikein"
    (with-redefs [environ.core/env {:ehoks-url "example.com/path/"}
                  oph.heratepalvelu.external.cas-client/get-service-ticket
                  mock-get-service-ticket
                  oph.heratepalvelu.external.http-client/get mock-client-get]
      (let [expected {:data
                      {:type "mock-client-call-response"
                       :params
                       {:method "get"
                        :uri "example.com/path/hoks/456/hankintakoulutukset"
                        :options
                        {:headers {:ticket {:type "cas-service-ticket"
                                            :service "/ehoks-virkailija-backend"
                                            :suffix "cas-security-check"}}
                         :as :json}}}}]
        (is (= (ehoks/get-hankintakoulutus-oids 456) expected))))))

(deftest test-add-lahetys-info-to-kyselytunnus
  (testing "Varmista, että add-lahetys-info-to-kyselytunnus toimii oikein"
    (with-redefs [environ.core/env {:ehoks-url "example.com/path/"}
                  oph.heratepalvelu.external.cas-client/get-service-ticket
                  mock-get-service-ticket]
      (let [data {:kyselylinkki "kysely.linkki/asdf"
                  :lahetyspvm "2021-12-15"
                  :sahkoposti "foo@bar.com"
                  :lahetystila (:ei-lahetetty c/kasittelytilat)}
            expected {:body
                      {:data
                       {:type "mock-client-call-response"
                        :params
                        {:method "patch"
                         :uri "example.com/path/hoks/kyselylinkki"
                         :options
                         {:headers {:ticket
                                    {:type "cas-service-ticket"
                                     :service "/ehoks-virkailija-backend"
                                     :suffix "cas-security-check"}}
                          :content-type "application/json"
                          :body (generate-string data)
                          :as :json}}}}}]
        (with-redefs [oph.heratepalvelu.external.http-client/patch
                      mock-client-patch]
          (is (= (ehoks/add-lahetys-info-to-kyselytunnus data) expected)))
        (with-redefs [oph.heratepalvelu.external.http-client/patch
                      mock-client-patch-with-error]
          (is (= (ehoks/add-lahetys-info-to-kyselytunnus data) expected)))
        (reset! has-errored? false)))))

(deftest test-patch-oht-tep-kasitelty
  (testing "Varmista, että patch-oht-tep-kasitelty toimii oikein"
    (with-redefs [environ.core/env {:ehoks-url "example.com/path/"}
                  oph.heratepalvelu.external.cas-client/get-service-ticket
                  mock-get-service-ticket
                  oph.heratepalvelu.external.http-client/patch
                  mock-client-patch]
      (let [expect {:body
                    {:data
                     {:type "mock-client-call-response"
                      :params
                      {:method "patch"
                       :uri (str "example.com/path/heratepalvelu/"
                                 "osaamisenhankkimistavat/11235/kasitelty")
                       :options
                       {:headers {:ticket {:type "cas-service-ticket"
                                           :service "/ehoks-virkailija-backend"
                                           :suffix "cas-security-check"}}
                        :content-type "application/json"
                        :as :json}}}}}]
        (is (= (ehoks/patch-oht-tep-kasitelty 11235) expect))))))

(deftest test-get-paattyneet-tyoelamajaksot
  (testing "Varmista, että get-paattyneet-tyoelamajaksot toimii oikein"
    (with-redefs [environ.core/env {:ehoks-url "example.com/path/"}
                  oph.heratepalvelu.external.cas-client/get-service-ticket
                  mock-get-service-ticket
                  oph.heratepalvelu.external.http-client/get mock-client-get]
      (let [expected {:body
                      {:data
                       {:type "mock-client-call-response"
                        :params
                        {:method "get"
                         :uri "example.com/path/heratepalvelu/tyoelamajaksot"
                         :options
                         {:headers
                          {:ticket {:type "cas-service-ticket"
                                    :service "/ehoks-virkailija-backend"
                                    :suffix "cas-security-check"}}
                          :query-params {:start "2021-08-08"
                                         :end "2021-09-09"
                                         :limit 100}
                          :as :json}}}}}]
        (is (= (ehoks/get-paattyneet-tyoelamajaksot "2021-08-08"
                                                    "2021-09-09"
                                                    100)
               expected))))))

(deftest test-get-retry-kyselylinkit
  (testing "Varmista, että get-retry-kyselylinkit toimii oikein"
    (with-redefs [environ.core/env {:ehoks-url "example.com/"}
                  oph.heratepalvelu.external.cas-client/get-service-ticket
                  mock-get-service-ticket
                  oph.heratepalvelu.external.http-client/get mock-client-get]
      (let [expected {:body
                      {:data
                       {:type "mock-client-call-response"
                        :params
                        {:method "get"
                         :uri (str "example.com/heratepalvelu/"
                                   "kasittelemattomat-heratteet")
                         :options
                         {:headers
                          {:ticket {:type "cas-service-ticket"
                                    :service "/ehoks-virkailija-backend"
                                    :suffix "cas-security-check"}}
                          :query-params {:start "2021-08-08"
                                         :end "2021-09-09"
                                         :limit 100}
                          :as :json}}}}}]
        (is (= (ehoks/get-retry-kyselylinkit "2021-08-08" "2021-09-09" 100)
               expected))))))

(deftest test-get-tyoelamajaksot-active-between
  (testing "Varmista, että get-tyoelamajaksot-active-between toimii oikein."
    (with-redefs [environ.core/env {:ehoks-url "example.com/"}
                  oph.heratepalvelu.external.cas-client/get-service-ticket
                  mock-get-service-ticket
                  oph.heratepalvelu.external.http-client/get mock-client-get]
      (let [expected {:type "mock-client-call-response"
                      :params
                      {:method "get"
                       :uri (str "example.com/heratepalvelu/"
                                 "tyoelamajaksot-active-between")
                       :options
                       {:headers
                        {:ticket {:type "cas-service-ticket"
                                  :service "/ehoks-virkailija-backend"
                                  :suffix "cas-security-check"}}
                        :query-params {:oppija "1.2.3.4"
                                       :start  "2021-08-01"
                                       :end    "2021-10-05"}
                        :as :json}}}]
        (is (= (ehoks/get-tyoelamajaksot-active-between "1.2.3.4"
                                                        "2021-08-01"
                                                        "2021-10-05")
               expected))))))

(deftest test-patch-amis-aloitus-ja-paattoheratteet-kasitellyt
  (testing (str "Varmista, että patch-amis-aloitusherate-kasitelty ja"
                "patch-amis-paattoherate-kasitelty toimivat oikein")
    (with-redefs [oph.heratepalvelu.external.ehoks/ehoks-patch
                  (fn [url] {:url url})]
      (is (= (ehoks/patch-amis-aloitusherate-kasitelty 34)
             {:url "heratepalvelu/hoksit/34/aloitusherate-kasitelty"}))
      (is (= (ehoks/patch-amis-paattoherate-kasitelty 56)
             {:url "heratepalvelu/hoksit/56/paattoherate-kasitelty"})))))
