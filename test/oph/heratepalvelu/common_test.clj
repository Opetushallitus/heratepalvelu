(ns oph.heratepalvelu.common-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.common :refer :all]
            [oph.heratepalvelu.test-util :refer :all]
            [clj-time.core :as t]
            [clojure.string :as str])
  (:import (java.time LocalDate)))

(deftest test-get-koulutustoimija-oid
  (testing "Get koulutustoimija oid"
    (with-redefs
      [oph.heratepalvelu.external.organisaatio/get-organisaatio
       mock-get-organisaatio]
      (do
        (is (= "1.2.246.562.10.346830761110"
               (get-koulutustoimija-oid
                 {:oid "1.2.246.562.15.43634207518"
                  :koulutustoimija {:oid "1.2.246.562.10.346830761110"}})))
        (is (= "1.2.246.562.10.346830761110"
               (get-koulutustoimija-oid
                 {:oid "1.2.246.562.15.43634207512"
                  :oppilaitos {:oid "1.2.246.562.10.52251087186"}})))))))

(deftest test-kausi
  (testing "Generate laskentakausi string"
    (is (= "2018-2019" (kausi "2018-07-01")))
    (is (= "2018-2019" (kausi "2019-06-30")))))

(deftest test-get-tila
  (testing "Get correct tila given opiskeluoikeus and vahvistus-pvm"
    (is (= (get-tila
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
              :tila {:opiskeluoikeusjaksot [{:alku "2019-07-24"
                                             :tila {:koodiarvo "lasna"}}
                                            {:alku "2019-07-23"
                                             :tila
                                             {:koodiarvo "valmistunut"}}]}}
             "2019-07-23")
           "valmistunut"))
    (is (= (get-tila
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
              :tila {:opiskeluoikeusjaksot [{:alku "2019-07-24"
                                             :tila {:koodiarvo "lasna"}}
                                            {:alku "2019-07-23"
                                             :tila
                                             {:koodiarvo "valmistunut"}}]}}
             "2019-07-24")
           "lasna"))))

(deftest check-opiskeluoikeus-tila-test
  (testing "Opiskeluoikeuden tilan tarkastus. Keskeytetty opiskeluoikeus estää
           jakson käsittelyn. Jakson päättymispäivänä keskeytetty opiskeluoikeus
           ei estä jakson käsittelyä."
    (let [loppupvm "2021-09-07"
          opiskeluoikeus-lasna {:tila
                                {:opiskeluoikeusjaksot
                                 [{:alku "2021-06-20"
                                   :tila {:koodiarvo "loma"}}
                                  {:alku "2021-05-01"
                                   :tila {:koodiarvo "lasna"}}
                                  {:alku "2021-06-25"
                                   :tila {:koodiarvo "lasna"}}]}}
          opiskeluoikeus-eronnut-samana-paivana {:tila
                                                 {:opiskeluoikeusjaksot
                                                  [{:alku "2021-06-20"
                                                    :tila {:koodiarvo "loma"}}
                                                   {:alku "2021-05-01"
                                                    :tila {:koodiarvo "lasna"}}
                                                   {:alku "2021-09-07"
                                                    :tila
                                                    {:koodiarvo "eronnut"}}]}}
          opiskeluoikeus-eronnut-tulevaisuudessa {:tila
                                                  {:opiskeluoikeusjaksot
                                                   [{:alku "2021-06-20"
                                                     :tila {:koodiarvo "loma"}}
                                                    {:alku "2021-05-01"
                                                     :tila {:koodiarvo "lasna"}}
                                                    {:alku "2021-09-08"
                                                     :tila
                                                     {:koodiarvo "eronnut"}}]}}
          opiskeluoikeus-eronnut-paivaa-aiemmin {:tila
                                                 {:opiskeluoikeusjaksot
                                                  [{:alku "2021-06-20"
                                                    :tila {:koodiarvo "loma"}}
                                                   {:alku "2021-05-01"
                                                    :tila {:koodiarvo "lasna"}}
                                                   {:alku "2021-09-06"
                                                    :tila
                                                    {:koodiarvo "eronnut"}}]}}]
      (is (= true (check-opiskeluoikeus-tila opiskeluoikeus-lasna loppupvm)))
      (is (= true (check-opiskeluoikeus-tila
                    opiskeluoikeus-eronnut-samana-paivana
                    loppupvm)))
      (is (= true (check-opiskeluoikeus-tila
                    opiskeluoikeus-eronnut-tulevaisuudessa
                    loppupvm)))
      (is (nil? (check-opiskeluoikeus-tila
                  opiskeluoikeus-eronnut-paivaa-aiemmin
                  loppupvm))))))

(deftest test-check-suoritus-type
  (testing "Check suoritus type"
    (is (false? (check-suoritus-type? {:tyyppi {:koodiarvo "valma"}})))
    (is (true? (check-suoritus-type?
                 {:tyyppi {:koodiarvo "ammatillinentutkintoosittainen"}})))
    (is (true? (check-suoritus-type?
                 {:tyyppi {:koodiarvo "ammatillinentutkinto"}})))))

(deftest test-check-opiskeluoikeus-suoritus-types
  (testing "Check opiskeluoikeus suoritus types"
    (is (nil? (check-opiskeluoikeus-suoritus-types?
                {:suoritukset [{:tyyppi {:koodiarvo "valma"}}]})))
    (is (true? (check-opiskeluoikeus-suoritus-types?
                 {:suoritukset
                  [{:tyyppi {:koodiarvo "ammatillinentutkinto"}}]})))
    (is (true? (check-opiskeluoikeus-suoritus-types?
                 {:suoritukset
                  [{:tyyppi {:koodiarvo "nayttotutkintoonvalmistavakoulutus"}}
                   {:tyyppi {:koodiarvo "ammatillinentutkintoosittainen"}}]})))
    (is (nil? (check-opiskeluoikeus-suoritus-types?
                {:suoritukset [{:tyyppi {:koodiarvo "valma"}}
                               {:tyyppi {:koodiarvo "telma"}}]})))))

(deftest test-date-string-to-timestamp
  (testing "Transforming date-string to timestamp"
    (is (= (date-string-to-timestamp "1970-01-01") 0))
    (is (= (date-string-to-timestamp "2019-08-01") 1564617600000))))

(deftest test-period-contains-date?
  (testing "Check whether a given date falls within one of a list of periods."
    (let [normal [{:alku "2022-03-03" :loppu "2022-03-07"}
                  {:alku "2022-04-01" :loppu "2022-05-01"}]
          no-alku [{:loppu "2022-06-06"}]
          no-loppu [{:alku "2022-01-01"}]]
      (is (true? (period-contains-date? normal "2022-03-05")))
      (is (true? (period-contains-date? normal "2022-04-04")))
      (is (false? (period-contains-date? normal "2022-03-10")))
      (is (true? (period-contains-date? no-alku "2022-01-03")))
      (is (false? (period-contains-date? no-alku "2022-07-07")))
      (is (true? (period-contains-date? no-loppu "2025-07-07")))
      (is (false? (period-contains-date? no-loppu "2021-12-31"))))))

(deftest test-is-maksuton?
  (testing "Check whether opiskeluoikeus is free (maksuton)."
    (let [opiskeluoikeus {:lisätiedot {:maksuttomuus [{:alku "2022-01-01"
                                                       :loppu "2022-06-30"
                                                       :maksuton true}
                                                      {:alku "2022-07-01"
                                                       :loppu "2022-12-31"
                                                       :maksuton false}]}}]
      (is (true? (is-maksuton? opiskeluoikeus "2022-02-02")))
      (is (false? (is-maksuton? opiskeluoikeus "2022-08-09"))))))

(deftest test-erityinen-tuki-voimassa?
  (testing "Check whether opiskeluoikeus has erityinen tuki."
    (let [opiskeluoikeus {:lisätiedot {:erityinenTuki [{:alku "2022-01-01"
                                                        :loppu "2022-06-30"}]}}]
      (is (true? (erityinen-tuki-voimassa? opiskeluoikeus "2022-02-02")))
      (is (false? (erityinen-tuki-voimassa? opiskeluoikeus "2022-08-09"))))))

(deftest test-has-time-to-answer
  (let [date1 (t/today)
        date2 (t/plus (t/now) (t/days 1))
        date3 (t/minus  (t/now) (t/days 1))]
    (is (true? (has-time-to-answer?
                 (str date1))))
    (is (true? (has-time-to-answer?
                 (str date2))))
    (is (false? (has-time-to-answer?
                  (str date3))))))

(deftest test-create-nipputunniste
  (testing "Create normalized nipputunniste"
    (is (str/starts-with? (create-nipputunniste "Ääkköspaikka, Crème brûlée")
                          "aakkospaikka_creme_brulee"))
    (is (str/starts-with? (create-nipputunniste "árvíztűrő tükörfúrógép")
                          "arvizturo_tukorfurogep"))))

(deftest test-check-valid-herate-date
  (testing "True if heratepvm is >= 2022-07-01"
    (is (true? (check-valid-herate-date "2022-07-02")))
    (is (true? (check-valid-herate-date "2022-07-01")))
    (is (not (true? (check-valid-herate-date "2022-06-01"))))
    (is (not (true? (check-valid-herate-date "2022-07-01xxxx"))))
    (is (not (true? (check-valid-herate-date ""))))
    (is (not (true? (check-valid-herate-date nil))))))

(deftest test-check-sisaltyy-opiskeluoikeuteen
  (testing "Check sisältyy opiskeluoikeuteen"
    (let [oo {:oid "1.2.246.562.15.43634207518"
              :sisältyyOpiskeluoikeuteen {:oid "1.2.246.562.15.12345678901"}}]
      (is (= nil (check-sisaltyy-opiskeluoikeuteen? oo))))))

(deftest test-has-nayttotutkintoonvalmistavakoulutus
  (testing "Check has-nayttotutkintoonvalmistavakoulutus"
    (let [oo1 {:suoritukset
               [{:tyyppi {:koodiarvo "nayttotutkintoonvalmistavakoulutus"}}]}
          oo2 {:suoritukset [{:tyyppi {:koodiarvo "ammatillinentutkinto"}}]}]
      (is (true? (has-nayttotutkintoonvalmistavakoulutus? oo1)))
      (is (= nil (has-nayttotutkintoonvalmistavakoulutus? oo2))))))

(deftest test-next-niputus-date
  (testing "Get next niputus date"
    (is (= (LocalDate/of 2021 12 16) (next-niputus-date "2021-12-03")))
    (is (= (LocalDate/of 2022 1 1) (next-niputus-date "2021-12-27")))
    (is (= (LocalDate/of 2021 5 1) (next-niputus-date "2021-04-25")))
    (is (= (LocalDate/of 2022 6 30) (next-niputus-date "2022-06-24")))))

(deftest test-create-update-item-options
  (testing "Create update item options"
    (is (= (create-update-item-options {:field-1 [:s "value"]
                                        :field-2 [:n 123]})
           {:update-expr "SET #field_1 = :field_1, #field_2 = :field_2"
            :expr-attr-names {"#field_1" "field-1"
                              "#field_2" "field-2"}
            :expr-attr-vals {":field_1" [:s "value"]
                             ":field_2" [:n 123]}}))))

(deftest test-valid-number?
  (testing
    "Funktio valid-number? tunnistaa oikeita ja virheellisiä puhelinnumeroja"
    (let [fi-phone-number "040 654 3210"
          fi-phone-number-intl-fmt "040 654 3210"
          intl-phone-number "+1 517 987 5432"
          junk-invalid "laksj fdaiu fd098098asdf"
          unicode-invalid "+358 40 987 6543à"]
      (is (valid-number? fi-phone-number))
      (is (valid-number? fi-phone-number-intl-fmt))
      (is (valid-number? intl-phone-number))
      (is (not (valid-number? junk-invalid)))
      (is (not (valid-number? unicode-invalid))))))

(deftest test-client-error?
  (testing
    "Funktio client-error? erottaa client erroreja muista HTTP-statuksista"
    (let [client-error (ex-info "File not found" {:status 404})
          server-error (ex-info "Internal server error" {:status 503})]
      (is (client-error? client-error))
      (is (not (client-error? server-error))))))

(defn- construct-opiskeluoikeus [jaksot]
  {:tila {:opiskeluoikeusjaksot
          (for [[alku tila rahoitus] jaksot]
            {:alku alku
             :tila {:koodiarvo tila}
             :opintojenRahoitus {:koodiarvo (str rahoitus)}})}})

(deftest test-feedback-collecting-prevented?
  (testing
    "Tunnistetaan oikein opiskeluoikeuden tila, jossa palautekyselyitä
    ei lähetetä."
    (are [input result] (-> input
                            (construct-opiskeluoikeus)
                            (feedback-collecting-prevented? "2021-07-15")
                            (= result))
      [["2021-07-30" "lasna" 3]] false
      [["2018-06-20" "valmistunut" 1]] false
      [["2020-06-20" "lasna" 14]] true
      [["2019-01-03" "lasna" 1] ["2022-09-01" "lasna" 1]] false
      [["2019-01-03" "lasna" 6] ["2019-09-01" "valmistunut" 6]] true
      [["2020-03-15" "lasna" 3] ["2019-09-01" "lasna" 6]] false
      [["2021-03-15" "valmistunut" 2]
       ["2020-03-15" "lasna" 14]
       ["2019-09-01" "lasna" 14]] false
      [["2019-01-03" "lasna" 5] ["2019-09-01" "lasna" 15]] true)
    (are [input date result] (-> input
                                 (construct-opiskeluoikeus)
                                 (feedback-collecting-prevented? date)
                                 (= result))
      [["2020-06-20" "lasna" 14]] "2019-01-01" true
      [["2021-03-15" "valmistunut" 2]
       ["2020-03-15" "lasna" 14]
       ["2019-09-01" "lasna" 14]] "2020-07-01" true
      [["2021-03-15" "valmistunut" 2]
       ["2020-03-15" "lasna" 14]
       ["2019-09-01" "lasna" 14]] "2019-12-01" true
      [["2021-03-15" "valmistunut" 2]
       ["2020-03-15" "lasna" 14]
       ["2019-09-01" "lasna" 14]] "2019-07-01" true)))
