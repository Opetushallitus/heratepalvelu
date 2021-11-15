(ns oph.heratepalvelu.common-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.common :refer :all]
            [oph.heratepalvelu.util :refer :all]
            [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clojure.string :as str])
  (:import (java.time LocalDate)))

;; Cheks for falsey value(nil or false)
(defn falsey? [x] (if x false true))

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
    (is (true? (check-suoritus-type? {:tyyppi {:koodiarvo "ammatillinentutkintoosittainen"}})))
    (is (true? (check-suoritus-type? {:tyyppi {:koodiarvo "ammatillinentutkinto"}})))))

(deftest test-check-opiskeluoikeus-suoritus-types
  (testing "Check opiskeluoikeus suoritus types"
    (is (nil? (check-opiskeluoikeus-suoritus-types? {:suoritukset [{:tyyppi {:koodiarvo "valma"}}]})))
    (is (true? (check-opiskeluoikeus-suoritus-types?
                 {:suoritukset [{:tyyppi {:koodiarvo "ammatillinentutkinto"}}]})))
    (is (true? (check-opiskeluoikeus-suoritus-types?
                 {:suoritukset [{:tyyppi {:koodiarvo "nayttotutkintoonvalmistavakoulutus"}}
                                {:tyyppi {:koodiarvo "ammatillinentutkintoosittainen"}}]})))
    (is (nil? (check-opiskeluoikeus-suoritus-types?
                 {:suoritukset [{:tyyppi {:koodiarvo "valma"}}
                                {:tyyppi {:koodiarvo "telma"}}]})))))

(deftest test-date-string-to-timestamp
  (testing "Transforming date-string to timestamp"
    (is (= (date-string-to-timestamp "1970-01-01") 0))
    (is (= (date-string-to-timestamp "2019-08-01") 1564617600000))))

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
    (is (str/starts-with?
          (create-nipputunniste "Ääkköspaikka, Crème brûlée") "aakkospaikka_creme_brulee"))
    (is (str/starts-with?
          (create-nipputunniste "árvíztűrő tükörfúrógép") "arvizturo_tukorfurogep"))))

(deftest test-check-valid-herate-date
  (testing "True if heratepvm is >= 2021-07-01"
    (is (true? (check-valid-herate-date "2021-07-02")))
    (is (true? (check-valid-herate-date "2021-07-01")))
    (is (falsey? (check-valid-herate-date "2021-06-01")))
    (is (falsey? (check-valid-herate-date "2021-07-01xxxx")))
    (is (falsey? (check-valid-herate-date "")))
    (is (falsey? (check-valid-herate-date nil)))))

(deftest test-check-sisaltyy-opiskeluoikeuteen
  (testing "Check sisältyy opiskeluoikeuteen"
    (let [oo {:oid "1.2.246.562.15.43634207518"
              :sisältyyOpiskeluoikeuteen {:oid "1.2.246.562.15.12345678901"}}]
      (is (= nil (check-sisaltyy-opiskeluoikeuteen? oo))))))

(deftest test-has-nayttotutkintoonvalmistavakoulutus
  (testing "Check has-nayttotutkintoonvalmistavakoulutus"
    (let [oo1 {:suoritukset [{:tyyppi {:koodiarvo "nayttotutkintoonvalmistavakoulutus"}}]}
          oo2 {:suoritukset [{:tyyppi {:koodiarvo "ammatillinentutkinto"}}]}]
      (is (true? (has-nayttotutkintoonvalmistavakoulutus? oo1)))
      (is (= nil (has-nayttotutkintoonvalmistavakoulutus? oo2))))))

(deftest test-next-niputus-date
  (testing "Get next niputus date"
    (is (= (LocalDate/of 2021 12 16) (next-niputus-date "2021-12-03")))
    (is (= (LocalDate/of 2022 1 1) (next-niputus-date "2021-12-27")))
    (is (= (LocalDate/of 2021 5 1) (next-niputus-date "2021-04-25")))))