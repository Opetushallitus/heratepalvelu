(ns oph.heratepalvelu.amis.UpdatedOpiskeluoikeusHandler-test
  (:require [clj-time.coerce :as ctc]
            [clojure.test :refer :all]
            [oph.heratepalvelu.amis.UpdatedOpiskeluoikeusHandler :refer :all]))

(deftest test-get-vahvistus-pvm
  (testing "Get vahvistus pvm"
    (is (= (get-vahvistus-pvm {:oid "1.2.246.562.15.82039738925"
                               :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
                               :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                                              :tyyppi {:koodiarvo "ammatillinentutkinto"}
                                              :vahvistus {:päivä "2019-07-24"}}]})
           "2019-07-24"))
    (is (nil? (get-vahvistus-pvm {:oid "1.2.246.562.15.82039738925"
                               :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
                               :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                                              :tyyppi {:koodiarvo "valma"}
                                              :vahvistus {:päivä "2019-07-24"}}]})))
    (is (nil? (get-vahvistus-pvm {:oid "1.2.246.562.15.82039738925"
                                  :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
                                  :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                                                 :tyyppi {:koodiarvo "valma"}}]})))
    (is (= (get-vahvistus-pvm {:oid "1.2.246.562.15.82039738925"
                               :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
                               :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                                              :tyyppi {:koodiarvo "nayttotutkintoonvalmistavakoulutus"}
                                              :vahvistus {:päivä "2019-07-24"}}
                                             {:suorituskieli {:koodiarvo "FI"}
                                              :tyyppi {:koodiarvo "ammatillinentutkinto"}
                                              :vahvistus {:päivä "2019-07-23"}}]})
           "2019-07-23"))))

(deftest test-get-kysely-type
  (testing "Get correct kyselytyyppi from suoritus"
    (is (= (get-kysely-type {:oid "1.2.246.562.15.82039738925"
                             :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
                             :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                                            :tyyppi {:koodiarvo "nayttotutkintoonvalmistavakoulutus"}
                                            :vahvistus {:päivä "2019-07-24"}}
                                           {:suorituskieli {:koodiarvo "FI"}
                                            :tyyppi {:koodiarvo "ammatillinentutkinto"}
                                            :vahvistus {:päivä "2019-07-23"}}]})
            "tutkinnon_suorittaneet"))
    (is (= (get-kysely-type {:oid "1.2.246.562.15.82039738925"
                             :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
                             :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                                            :tyyppi {:koodiarvo "nayttotutkintoonvalmistavakoulutus"}
                                            :vahvistus {:päivä "2019-07-24"}}
                                           {:suorituskieli {:koodiarvo "FI"}
                                            :tyyppi {:koodiarvo "ammatillinentutkintoosittainen"}
                                            :vahvistus {:päivä "2019-07-23"}}]})
           "tutkinnon_osia_suorittaneet"))
    (is (nil? (get-kysely-type {:oid "1.2.246.562.15.82039738925"
                                :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
                                :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                                               :tyyppi {:koodiarvo "valma"}}]})))))

(deftest test-get-tila
  (testing "Get correct tila given opiskeluoikeus and vahvistus-pvm"
    (is (= (get-tila {:oid "1.2.246.562.15.82039738925"
                      :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
                      :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                                     :tyyppi {:koodiarvo "nayttotutkintoonvalmistavakoulutus"}
                                     :vahvistus {:päivä "2019-07-24"}}
                                    {:suorituskieli {:koodiarvo "FI"}
                                     :tyyppi {:koodiarvo "ammatillinentutkintoosittainen"}
                                     :vahvistus {:päivä "2019-07-23"}}]
                      :tila {:opiskeluoikeusjaksot [{:alku "2019-07-24"
                                                     :tila {:koodiarvo "lasna"}}
                                                    {:alku "2019-07-23"
                                                     :tila {:koodiarvo "valmistunut"}}]}}
                     "2019-07-23")
           "valmistunut"))
    (is (= (get-tila {:oid "1.2.246.562.15.82039738925"
                      :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
                      :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                                     :tyyppi {:koodiarvo "nayttotutkintoonvalmistavakoulutus"}
                                     :vahvistus {:päivä "2019-07-24"}}
                                    {:suorituskieli {:koodiarvo "FI"}
                                     :tyyppi {:koodiarvo "ammatillinentutkintoosittainen"}
                                     :vahvistus {:päivä "2019-07-23"}}]
                      :tila {:opiskeluoikeusjaksot [{:alku "2019-07-24"
                                                     :tila {:koodiarvo "lasna"}}
                                                    {:alku "2019-07-23"
                                                     :tila {:koodiarvo "valmistunut"}}]}}
                     "2019-07-24")
           "lasna"))))

(deftest test-check-tila
  (testing "Check for acceptable tila given opiskeluoikeus and vahvistus-pvm"
    (is (check-tila {:oid "1.2.246.562.15.82039738925"
                     :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
                     :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                                    :tyyppi {:koodiarvo "nayttotutkintoonvalmistavakoulutus"}
                                    :vahvistus {:päivä "2019-07-24"}}
                                   {:suorituskieli {:koodiarvo "FI"}
                                    :tyyppi {:koodiarvo "ammatillinentutkintoosittainen"}
                                    :vahvistus {:päivä "2019-07-23"}}]
                     :tila {:opiskeluoikeusjaksot [{:alku "2019-07-24"
                                                    :tila {:koodiarvo "lasna"}}
                                                   {:alku "2019-07-23"
                                                    :tila {:koodiarvo "valmistunut"}}]}}
                     "2019-07-23"))
    (is (check-tila {:oid "1.2.246.562.15.82039738925"
                     :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
                     :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                                    :tyyppi {:koodiarvo "nayttotutkintoonvalmistavakoulutus"}
                                    :vahvistus {:päivä "2019-07-24"}}
                                   {:suorituskieli {:koodiarvo "FI"}
                                    :tyyppi {:koodiarvo "ammatillinentutkintoosittainen"}
                                    :vahvistus {:päivä "2019-07-23"}}]
                     :tila {:opiskeluoikeusjaksot [{:alku "2019-07-24"
                                                    :tila {:koodiarvo "lasna"}}
                                                   {:alku "2019-07-23"
                                                    :tila {:koodiarvo "valmistunut"}}]}}
                    "2019-07-24"))
    (is (not (check-tila {:oid "1.2.246.562.15.82039738925"
                          :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
                          :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                                         :tyyppi {:koodiarvo "nayttotutkintoonvalmistavakoulutus"}
                                         :vahvistus {:päivä "2019-07-24"}}
                                        {:suorituskieli {:koodiarvo "FI"}
                                         :tyyppi {:koodiarvo "ammatillinentutkintoosittainen"}
                                         :vahvistus {:päivä "2019-07-23"}}]
                          :tila {:opiskeluoikeusjaksot [{:alku "2019-07-24"
                                                         :tila {:koodiarvo "katsotaaneronneeksi"}}
                                                        {:alku "2019-07-23"
                                                         :tila {:koodiarvo "valmistunut"}}]}}
                         "2019-07-24")))))

(deftest test-parse-herate
  (testing "Varmista, että parse-herate luo oikeanlaisen objektin"
    (let [hoks {:id "1234.15.67890"
                :opiskeluoikeus-oid "123.456.789"
                :oppija-oid "987.654.321"
                :sahkoposti "a@b.com"}
          kyselytyyppi "aloittaneet"
          alkupvm "2021-10-10"
          expected {:ehoks-id "1234.15.67890"
                    :kyselytyyppi "aloittaneet"
                    :opiskeluoikeus-oid "123.456.789"
                    :oppija-oid "987.654.321"
                    :sahkoposti "a@b.com"
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
