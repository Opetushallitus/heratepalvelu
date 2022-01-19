(ns oph.heratepalvelu.amis.UpdatedOpiskeluoikeusHandler-test
  (:require [clj-time.coerce :as ctc]
            [clojure.test :refer :all]
            [oph.heratepalvelu.amis.UpdatedOpiskeluoikeusHandler :refer :all]
            [oph.heratepalvelu.test-util :as tu]))

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

(def test-handleUpdatedOpiskeluoikeus-results (atom ""))

(defn- mock-get-item [query-params table]
  (when (and (= :s (first (:key query-params))) (= table "metadata-table-name"))
    (if (= "opiskeluoikeus-last-checked" (second (:key query-params)))
      {:key "opiskeluoikeus-last-checked"
       :value "2021-12-14T10:30:00.000Z"}
      {:key "opiskeluoikeus-last-page"
       :value "0"})))

(defn- mock-get-updated-opiskeluoikeudet [last-checked last-page]
  (reset! test-handleUpdatedOpiskeluoikeus-results
          (str @test-handleUpdatedOpiskeluoikeus-results
               last-checked
               " "
               last-page
               " "))
  [{:oid "1.2.3"}])

(defn- mock-get-koulutustoimija-oid [opiskeluoikeus]
  (reset! test-handleUpdatedOpiskeluoikeus-results
          (str @test-handleUpdatedOpiskeluoikeus-results opiskeluoikeus " "))
  "test-koulutustoimija-oid")

(defn- mock-get-vahvistus-pvm [opiskeluoikeus]
  (reset! test-handleUpdatedOpiskeluoikeus-results
          (str @test-handleUpdatedOpiskeluoikeus-results opiskeluoikeus " "))
  "2021-10-10")

(defn- mock-check-valid-herate-date [vahvistus-pvm]
  (reset! test-handleUpdatedOpiskeluoikeus-results
          (str @test-handleUpdatedOpiskeluoikeus-results vahvistus-pvm " "))
  true)

(defn- mock-check-organisaatio-whitelist? [koulutustoimija vahvistus-pvm]
  (reset! test-handleUpdatedOpiskeluoikeus-results
          (str @test-handleUpdatedOpiskeluoikeus-results
               koulutustoimija
               " "
               vahvistus-pvm
               " "))
  true)

(defn- mock-check-tila [opiskeluoikeus vahvistus-pvm]
  (reset! test-handleUpdatedOpiskeluoikeus-results
          (str @test-handleUpdatedOpiskeluoikeus-results
               opiskeluoikeus
               " "
               vahvistus-pvm
               " "))
  true)

(defn- mock-get-hoks-by-opiskeluoikeus [opiskeluoikeus-oid]
  (reset! test-handleUpdatedOpiskeluoikeus-results
          (str @test-handleUpdatedOpiskeluoikeus-results
               "get-hoks-by-opiskeluoikeus: "
               opiskeluoikeus-oid
               " "))

  {:osaamisen-hankkimisen-tarve true
   :id "123.456.789"
   :opiskeluoikeus-oid "1.2.3"
   :oppija-oid "7.8.9"
   :sahkoposti "a@b.com"})

(defn- mock-get-kysely-type [opiskeluoikeus]
  (reset! test-handleUpdatedOpiskeluoikeus-results
          (str @test-handleUpdatedOpiskeluoikeus-results "get-kysely-type "))
  "tutkinnon_suorittaneet")

(defn- mock-save-herate [herate opiskeluoikeus koulutustoimija]
  (reset! test-handleUpdatedOpiskeluoikeus-results
          (str @test-handleUpdatedOpiskeluoikeus-results
               "save-herate: "
               herate
               " "
               opiskeluoikeus
               " "
               koulutustoimija
               " ")))

(defn- mock-update-last-page [last-page]
  (reset! test-handleUpdatedOpiskeluoikeus-results
          (str @test-handleUpdatedOpiskeluoikeus-results
               "update-last-page: "
               last-page
               " ")))

(defn- mock-update-last-checked [start-time]
  (reset! test-handleUpdatedOpiskeluoikeus-results
          (str @test-handleUpdatedOpiskeluoikeus-results
               "update-last-checked: "
               start-time
               " ")))

(deftest test-handleUpdatedOpiskeluoikeus
  (testing "Varmista, että -handleUpdatedOpiskeluoikeus tekee kutsuja oikein"
    (with-redefs
      [environ.core/env {:metadata-table "metadata-table-name"}
       oph.heratepalvelu.amis.AMISCommon/save-herate mock-save-herate
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
       oph.heratepalvelu.common/check-organisaatio-whitelist?
       mock-check-organisaatio-whitelist?
       oph.heratepalvelu.common/check-valid-herate-date
       mock-check-valid-herate-date
       oph.heratepalvelu.common/get-koulutustoimija-oid
       mock-get-koulutustoimija-oid
       oph.heratepalvelu.db.dynamodb/get-item mock-get-item
       oph.heratepalvelu.external.ehoks/get-hoks-by-opiskeluoikeus
       mock-get-hoks-by-opiskeluoikeus
       oph.heratepalvelu.external.koski/get-updated-opiskeluoikeudet
       mock-get-updated-opiskeluoikeudet]
      (let [event (tu/mock-handler-event :scheduledherate)
            context (tu/mock-handler-context)
            expected (str "2021-12-14T10:30:00.000Z 0 {:oid \"1.2.3\"} "
                          "{:oid \"1.2.3\"} 2021-10-10 "
                          "test-koulutustoimija-oid 1633824000000 "
                          "{:oid \"1.2.3\"} 2021-10-10 "
                          "get-hoks-by-opiskeluoikeus: 1.2.3 "
                          "get-kysely-type save-herate: "
                          "{:ehoks-id \"123.456.789\", "
                          ":kyselytyyppi \"tutkinnon_suorittaneet\", "
                          ":opiskeluoikeus-oid \"1.2.3\", "
                          ":oppija-oid \"7.8.9\", :sahkoposti \"a@b.com\", "
                          ":alkupvm \"2021-10-10\"} {:oid \"1.2.3\"} "
                          "test-koulutustoimija-oid update-last-page: 1 ")]
        (-handleUpdatedOpiskeluoikeus {} event context)
        (is (= @test-handleUpdatedOpiskeluoikeus-results expected))))))
