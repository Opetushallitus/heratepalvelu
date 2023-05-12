(ns oph.heratepalvelu.amis.UpdatedOpiskeluoikeusHandler-test
  (:require [clj-time.coerce :as ctc]
            [clojure.test :refer :all]
            [clojure.data :refer [diff]]
            [oph.heratepalvelu.amis.UpdatedOpiskeluoikeusHandler :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.test-util :as tu]))

(deftest test-get-vahvistus-pvm
  (testing "Get vahvistus pvm"
    (is (= (get-vahvistus-pvm
             {:oid "1.2.246.562.15.82039738925"
              :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
              :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                             :tyyppi {:koodiarvo "ammatillinentutkinto"}
                             :vahvistus {:päivä "2019-07-24"}}]})
           "2019-07-24"))
    (is (nil? (get-vahvistus-pvm
                {:oid "1.2.246.562.15.82039738925"
                 :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
                 :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                                :tyyppi {:koodiarvo "valma"}
                                :vahvistus {:päivä "2019-07-24"}}]})))
    (is (nil? (get-vahvistus-pvm
                {:oid "1.2.246.562.15.82039738925"
                 :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
                 :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                                :tyyppi {:koodiarvo "valma"}}]})))
    (is (= (get-vahvistus-pvm
             {:oid "1.2.246.562.15.82039738925"
              :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
              :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                             :vahvistus {:päivä "2019-07-24"}
                             :tyyppi
                             {:koodiarvo "nayttotutkintoonvalmistavakoulutus"}}
                            {:suorituskieli {:koodiarvo "FI"}
                             :tyyppi {:koodiarvo "ammatillinentutkinto"}
                             :vahvistus {:päivä "2019-07-23"}}]})
           "2019-07-23"))))

(deftest test-get-kysely-type
  (testing "Get correct kyselytyyppi from suoritus"
    (is (= (get-kysely-type
             {:oid "1.2.246.562.15.82039738925"
              :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
              :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                             :tyyppi
                             {:koodiarvo "nayttotutkintoonvalmistavakoulutus"}
                             :vahvistus {:päivä "2019-07-24"}}
                            {:suorituskieli {:koodiarvo "FI"}
                             :tyyppi {:koodiarvo "ammatillinentutkinto"}
                             :vahvistus {:päivä "2019-07-23"}}]})
           "tutkinnon_suorittaneet"))
    (is (= (get-kysely-type
             {:oid "1.2.246.562.15.82039738925"
              :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
              :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                             :tyyppi
                             {:koodiarvo "nayttotutkintoonvalmistavakoulutus"}
                             :vahvistus {:päivä "2019-07-24"}}
                            {:suorituskieli {:koodiarvo "FI"}
                             :tyyppi
                             {:koodiarvo "ammatillinentutkintoosittainen"}
                             :vahvistus {:päivä "2019-07-23"}}]})
           "tutkinnon_osia_suorittaneet"))
    (is (nil? (get-kysely-type
                {:oid "1.2.246.562.15.82039738925"
                 :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
                 :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                                :tyyppi {:koodiarvo "valma"}}]})))))

(deftest test-check-tila
  (testing "Check for acceptable tila given opiskeluoikeus and vahvistus-pvm"
    (is (check-tila
          {:oid "1.2.246.562.15.82039738925"
           :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
           :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                          :tyyppi
                          {:koodiarvo "nayttotutkintoonvalmistavakoulutus"}
                          :vahvistus {:päivä "2019-07-24"}}
                         {:suorituskieli {:koodiarvo "FI"}
                          :tyyppi {:koodiarvo "ammatillinentutkintoosittainen"}
                          :vahvistus {:päivä "2019-07-23"}}]
           :tila {:opiskeluoikeusjaksot [{:alku "2019-07-24"
                                          :tila {:koodiarvo "lasna"}}
                                         {:alku "2019-07-23"
                                          :tila {:koodiarvo "valmistunut"}}]}}
          "2019-07-23"))
    (is (check-tila
          {:oid "1.2.246.562.15.82039738925"
           :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
           :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                          :tyyppi
                          {:koodiarvo "nayttotutkintoonvalmistavakoulutus"}
                          :vahvistus {:päivä "2019-07-24"}}
                         {:suorituskieli {:koodiarvo "FI"}
                          :tyyppi {:koodiarvo "ammatillinentutkintoosittainen"}
                          :vahvistus {:päivä "2019-07-23"}}]
           :tila {:opiskeluoikeusjaksot [{:alku "2019-07-24"
                                          :tila {:koodiarvo "lasna"}}
                                         {:alku "2019-07-23"
                                          :tila {:koodiarvo "valmistunut"}}]}}
          "2019-07-24"))
    (is (not (check-tila
               {:oid "1.2.246.562.15.82039738925"
                :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
                :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                               :tyyppi
                               {:koodiarvo "nayttotutkintoonvalmistavakoulutus"}
                               :vahvistus {:päivä "2019-07-24"}}
                              {:suorituskieli {:koodiarvo "FI"}
                               :tyyppi
                               {:koodiarvo "ammatillinentutkintoosittainen"}
                               :vahvistus {:päivä "2019-07-23"}}]
                :tila {:opiskeluoikeusjaksot
                       [{:alku "2019-07-24"
                         :tila {:koodiarvo "katsotaaneronneeksi"}}
                        {:alku "2019-07-23"
                         :tila {:koodiarvo "valmistunut"}}]}}
               "2019-07-24")))))

(deftest test-parse-herate
  (testing "Varmista, että parse-herate luo oikeanlaisen objektin"
    (let [hoks {:id "1234.15.67890"
                :opiskeluoikeus-oid "123.456.789"
                :oppija-oid "987.654.321"
                :sahkoposti "a@b.com"
                :puhelinnumero "1234567"}
          kyselytyyppi "aloittaneet"
          alkupvm "2021-10-10"
          expected {:ehoks-id "1234.15.67890"
                    :kyselytyyppi "aloittaneet"
                    :opiskeluoikeus-oid "123.456.789"
                    :oppija-oid "987.654.321"
                    :sahkoposti "a@b.com"
                    :puhelinnumero "1234567"
                    :alkupvm "2021-10-10"}]
      (is (= (parse-herate hoks kyselytyyppi alkupvm) expected)))))

(def test-update-last-checked-results (atom {}))

(defn- mock-update-last-checked-update-item [query-params options table]
  (when (and (= :s (first (:key query-params)))
             (= "opiskeluoikeus-last-checked" (second (:key query-params)))
             (= :s (first (get (:expr-attr-vals options) ":value"))))
    (reset! test-update-last-checked-results
            {:time-with-buffer (second (get (:expr-attr-vals options) ":value"))
             :table table})))

(deftest test-update-last-checked
  (testing "Varmista, että update-last-checked kutsuu update-item oikein"
    (with-redefs [environ.core/env {:metadata-table "metadata-table-name"}
                  oph.heratepalvelu.db.dynamodb/update-item
                  mock-update-last-checked-update-item]
      (let [datetime (ctc/from-long 1639478100000)
            expected {:time-with-buffer "2021-12-14T10:30:00.000Z"
                      :table "metadata-table-name"}]
        (update-last-checked datetime)
        (is (= @test-update-last-checked-results expected))))))

(def test-update-last-page-results (atom {}))

(defn- mock-update-last-page-update-item [query-params options table]
  (when (and (= :s (first (:key query-params)))
             (= "opiskeluoikeus-last-page" (second (:key query-params)))
             (= :s (first (get (:expr-attr-vals options) ":value"))))
    (reset! test-update-last-page-results
            {:page (second (get (:expr-attr-vals options) ":value"))
             :table table})))

(deftest test-update-last-page
  (testing "Varmista, että update-last-page kutsuu update-item oikein"
    (with-redefs [environ.core/env {:metadata-table "metadata-table-name"}
                  oph.heratepalvelu.db.dynamodb/update-item
                  mock-update-last-page-update-item]
      (let [page "test-page"
            expected {:page "test-page"
                      :table "metadata-table-name"}]
        (update-last-page page)
        (is (= @test-update-last-page-results expected))))))

(def test-handleUpdatedOpiskeluoikeus-results (atom []))

(defn mock-function [fn-name return-fn]
  (fn [& args]
    (swap! test-handleUpdatedOpiskeluoikeus-results
           #(conj % (into [(keyword fn-name)] args)))
    (apply return-fn args)))

(defn- mock-get-item [query-params table]
  (when (and (= :s (first (:key query-params))) (= table "metadata-table-name"))
    (if (= "opiskeluoikeus-last-checked" (second (:key query-params)))
      {:key "opiskeluoikeus-last-checked"
       :value "2021-12-14T10:30:00.000Z"}
      {:key "opiskeluoikeus-last-page"
       :value "0"})))

(def mock-get-updated-opiskeluoikeudet
  (mock-function 'mock-get-updated-opiskeluoikeudet
                 (constantly [{:oid "1.2.3"}])))

(def mock-get-koulutustoimija-oid
  (mock-function 'mock-get-koulutustoimija-oid
                 (constantly "test-koulutustoimija-oid")))

(def mock-get-vahvistus-pvm
  (mock-function 'mock-get-vahvistus-pvm (constantly "2021-10-10")))

(def mock-check-valid-herate-date
  (mock-function 'mock-check-valid-herate-date (constantly true)))

(def mock-whitelisted-organisaatio?!
  (mock-function 'mock-whitelisted-organisaatio?! (constantly true)))

(def mock-check-tila
  (mock-function 'mock-check-tila (constantly true)))

(def mock-get-hoks-by-opiskeluoikeus
  (mock-function 'mock-get-hoks-by-opiskeluoikeus
                 (constantly {:osaamisen-hankkimisen-tarve true
                              :id "123.456.789"
                              :opiskeluoikeus-oid "1.2.3"
                              :oppija-oid "7.8.9"
                              :sahkoposti "a@b.com"
                              :puhelinnumero "1234567"})))

(def mock-get-kysely-type
  (mock-function 'mock-get-kysely-type
                 (constantly "tutkinnon_suorittaneet")))

(def mock-save-herate
  (mock-function 'mock-save-herate (constantly nil)))

(def mock-update-last-page
  (mock-function 'mock-update-last-page (constantly nil)))

(def mock-update-last-checked
  (mock-function 'mock-update-last-checked (constantly nil)))

(deftest test-handleUpdatedOpiskeluoikeus
  (testing "Varmista, että -handleUpdatedOpiskeluoikeus tekee kutsuja oikein"
    (with-redefs
      [environ.core/env {:metadata-table "metadata-table-name"}
       oph.heratepalvelu.amis.AMISCommon/check-and-save-herate! mock-save-herate
       oph.heratepalvelu.amis.UpdatedOpiskeluoikeusHandler/check-tila
       mock-check-tila
       oph.heratepalvelu.amis.UpdatedOpiskeluoikeusHandler/get-kysely-type
       mock-get-kysely-type
       oph.heratepalvelu.amis.UpdatedOpiskeluoikeusHandler/get-vahvistus-pvm
       mock-get-vahvistus-pvm
       oph.heratepalvelu.amis.UpdatedOpiskeluoikeusHandler/update-last-checked
       mock-update-last-checked
       oph.heratepalvelu.amis.UpdatedOpiskeluoikeusHandler/update-last-page
       mock-update-last-page
       c/whitelisted-organisaatio?! mock-whitelisted-organisaatio?!
       c/valid-herate-date? mock-check-valid-herate-date
       c/get-koulutustoimija-oid mock-get-koulutustoimija-oid
       oph.heratepalvelu.db.dynamodb/get-item mock-get-item
       oph.heratepalvelu.external.ehoks/get-hoks-by-opiskeluoikeus
       mock-get-hoks-by-opiskeluoikeus
       oph.heratepalvelu.external.koski/get-updated-opiskeluoikeudet
       mock-get-updated-opiskeluoikeudet]
      (let [event (tu/mock-handler-event :scheduledherate)
            context (tu/mock-handler-context)
            expected
            [[:mock-get-updated-opiskeluoikeudet "2021-12-14T10:30:00.000Z" 0]
             [:mock-get-koulutustoimija-oid {:oid "1.2.3"}]
             [:mock-get-vahvistus-pvm {:oid "1.2.3"}]
             [:mock-check-valid-herate-date "2021-10-10"]
             [:mock-whitelisted-organisaatio?!
              "test-koulutustoimija-oid" 1633824000000]
             [:mock-check-tila {:oid "1.2.3"} "2021-10-10"]
             [:mock-get-hoks-by-opiskeluoikeus "1.2.3"]
             [:mock-get-kysely-type {:oid "1.2.3"}]
             [:mock-save-herate
              {:ehoks-id "123.456.789"
               :kyselytyyppi "tutkinnon_suorittaneet"
               :opiskeluoikeus-oid "1.2.3"
               :oppija-oid "7.8.9"
               :sahkoposti "a@b.com"
               :puhelinnumero "1234567"
               :alkupvm "2021-10-10"}
              {:oid "1.2.3"}
              "test-koulutustoimija-oid"
              "tiedot_muuttuneet_koskessa"]
             [:mock-update-last-page 1]]]
        (-handleUpdatedOpiskeluoikeus {} event context)
        (is (= @test-handleUpdatedOpiskeluoikeus-results expected)
            (->> (diff @test-handleUpdatedOpiskeluoikeus-results expected)
                 (clojure.string/join "\n")
                 (str "Eroavaisuudet: \n")))))))
