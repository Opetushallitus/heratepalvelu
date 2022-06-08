(ns oph.heratepalvelu.common-test
  (:require [clojure.string :as str]
            [clojure.test :refer :all]
            [oph.heratepalvelu.common :refer :all]
            [oph.heratepalvelu.test-util :refer :all])
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

(deftest test-has-time-to-answer
  (is (true? (has-time-to-answer? (str (LocalDate/now)))))
  (is (true? (has-time-to-answer? (str (.plusDays (LocalDate/now) 1)))))
  (is (false? (has-time-to-answer? (str (.minusDays (LocalDate/now) 1))))))

(deftest test-create-nipputunniste
  (testing "Create normalized nipputunniste"
    (is (str/starts-with? (create-nipputunniste "Ääkköspaikka, Crème brûlée")
                          "aakkospaikka_creme_brulee"))
    (is (str/starts-with? (create-nipputunniste "árvíztűrő tükörfúrógép")
                          "arvizturo_tukorfurogep"))))

(deftest test-check-valid-herate-date
  (testing "True if heratepvm is >= 2021-07-01"
    (is (true? (check-valid-herate-date "2021-07-02")))
    (is (true? (check-valid-herate-date "2021-07-01")))
    (is (not (true? (check-valid-herate-date "2021-06-01"))))
    (is (not (true? (check-valid-herate-date "2021-07-01xxxx"))))
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
