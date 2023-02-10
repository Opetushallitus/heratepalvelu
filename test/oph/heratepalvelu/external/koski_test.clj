(ns oph.heratepalvelu.external.koski-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.external.koski :as koski])
  (:import (clojure.lang ExceptionInfo)))

(defn- mock-get-opiskeluoikeus-client-get [uri options]
  {:body {:uri uri :options options}})

(deftest test-get-opiskeluoikeus
  (testing "Varmista, että get-opiskeluoikeus toimii oikein"
    (with-redefs [environ.core/env {:koski-url "example.com/koski"
                                    :koski-user "user"}
                  oph.heratepalvelu.external.http-client/get
                  mock-get-opiskeluoikeus-client-get
                  oph.heratepalvelu.external.koski/pwd (delay "secret")]
      (let [expected {:uri "example.com/koski/opiskeluoikeus/1.2.3.4"
                      :options {:basic-auth ["user" "secret"] :as :json}}]
        (is (= (koski/get-opiskeluoikeus "1.2.3.4") expected))))))

(defn- mock-throws-404 [_] (throw (ex-info "404 error" {:status 404})))
(defn- mock-throws-other-error [_] (throw (ex-info "Random error" {})))

(deftest test-get-opiskeluoikeus-catch-404
  (testing "get-opiskeluoikeus-catch-404 käsittelee virheitä oikein"
    (with-redefs [oph.heratepalvelu.external.koski/get-opiskeluoikeus
                  mock-throws-404]
      (is (nil? (koski/get-opiskeluoikeus-catch-404! "1.2.3"))))
    (with-redefs [oph.heratepalvelu.external.koski/get-opiskeluoikeus
                  mock-throws-other-error]
      (is (thrown? ExceptionInfo (koski/get-opiskeluoikeus-catch-404! "1.2"))))))

(def test-get-updated-opiskeluoikeudet-saved-params (atom {}))

(defn- mock-get-updated-opiskeluoikeudet-client-get [uri options]
  (reset! test-get-updated-opiskeluoikeudet-saved-params {:uri uri
                                                          :options options})
  {:body [{:opiskeluoikeudet [{:aikaleima "2021-01-01"}
                              {:aikaleima "2020-04-04"}]}
          {:opiskeluoikeudet [{:aikaleima "2020-11-11"}
                              {:aikaleima "2022-01-25"}
                              {:aikaleima "2021-05-05"}]}]})

(deftest test-get-updated-opiskeluoikeudet
  (testing "get-updated-opiskeluoikeudet tekee oikean kutsun ja käsittelyn"
    (with-redefs [environ.core/env {:koski-url "example.com/koski"
                                    :koski-user "user"}
                  oph.heratepalvelu.external.http-client/get
                  mock-get-updated-opiskeluoikeudet-client-get
                  oph.heratepalvelu.external.koski/pwd (delay "secret")]
      (let [expected [{:aikaleima "2020-04-04"}
                      {:aikaleima "2020-11-11"}
                      {:aikaleima "2021-01-01"}
                      {:aikaleima "2021-05-05"}
                      {:aikaleima "2022-01-25"}]
            expected-saved {:uri "example.com/koski/oppija/"
                            :options
                            {:query-params
                             {"opiskeluoikeudenTyyppi" "ammatillinenkoulutus"
                              "muuttunutJälkeen" "2021-12-15"
                              "pageSize" 100
                              "pageNumber" 3}
                             :basic-auth ["user" "secret"]
                             :as :json-strict}}]
        (is (= expected (koski/get-updated-opiskeluoikeudet "2021-12-15" 3)))
        (is (= expected-saved
               @test-get-updated-opiskeluoikeudet-saved-params))))))
