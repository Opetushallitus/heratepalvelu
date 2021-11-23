(ns oph.heratepalvelu.tpk.tpkNiputusHandler-test
  (:require [clj-time.core :as t]
            [clojure.test :refer :all]
            [oph.heratepalvelu.tpk.tpkNiputusHandler :as tpk]))

(deftest test-check-jakso
  (testing "check-jakso? tarkistaa jaksot oikein"
    (let [good-jakso1 {:koulutustoimija        "1.2.246.562.10.346830761110"
                       :tyopaikan_nimi         "Testi työpaikka"
                       :tyopaikan_ytunnus      "1234567-8"
                       :jakso_loppupvm         "2021-11-20"
                       :hankkimistapa_tyyppi   "koulutussopimus"}
          good-jakso2 {:koulutustoimija        "1.2.246.562.10.346830761110"
                       :tyopaikan_nimi         "Testi työpaikka"
                       :tyopaikan_ytunnus      "1234567-8"
                       :jakso_loppupvm         "2021-11-20"
                       :hankkimistapa_tyyppi   "oppisopimus"
                       :oppisopimuksen_perusta "01"}
          good-jakso3 {:koulutustoimija        "1.2.246.562.10.346830761110"
                       :tyopaikan_nimi         "Testi työpaikka"
                       :tyopaikan_ytunnus      "1234567-8"
                       :jakso_loppupvm         "2021-11-20"
                       :hankkimistapa_tyyppi   "oppisopimus"}
          bad-jakso1 {:koulutustoimija        "1.2.246.562.10.346830761110"
                      :tyopaikan_nimi         "Testi työpaikka"
                      :tyopaikan_ytunnus      "1234567-8"
                      :jakso_loppupvm         "2021-11-20"
                      :hankkimistapa_tyyppi   "oppisopimus"
                      :oppisopimuksen_perusta "02"}
          bad-jakso2 {:koulutustoimija        "1.2.246.562.10.346830761110"
                      :tyopaikan_ytunnus      "1234567-8"
                      :jakso_loppupvm         "2021-11-20"
                      :hankkimistapa_tyyppi   "oppisopimus"
                      :oppisopimuksen_perusta "01"}
          bad-jakso3 {:tyopaikan_nimi         "Testi työpaikka"
                      :tyopaikan_ytunnus      "1234567-8"
                      :jakso_loppupvm         "2021-11-20"
                      :hankkimistapa_tyyppi   "oppisopimus"
                      :oppisopimuksen_perusta "01"}
          bad-jakso4 {:koulutustoimija        "1.2.246.562.10.346830761110"
                      :tyopaikan_nimi         "Testi työpaikka"
                      :jakso_loppupvm         "2021-11-20"
                      :hankkimistapa_tyyppi   "oppisopimus"
                      :oppisopimuksen_perusta "01"}
          bad-jakso5 {:koulutustoimija        "1.2.246.562.10.346830761110"
                      :tyopaikan_nimi         "Testi työpaikka"
                      :tyopaikan_ytunnus      "1234567-8"
                      :hankkimistapa_tyyppi   "oppisopimus"
                      :oppisopimuksen_perusta "01"}]
      (is (tpk/check-jakso? good-jakso1))
      (is (tpk/check-jakso? good-jakso2))
      (is (tpk/check-jakso? good-jakso3))
      (is (not (tpk/check-jakso? bad-jakso1)))
      (is (not (tpk/check-jakso? bad-jakso2)))
      (is (not (tpk/check-jakso? bad-jakso3)))
      (is (not (tpk/check-jakso? bad-jakso4)))
      (is (not (tpk/check-jakso? bad-jakso5))))))

(deftest test-get-kausi-alkupvm-loppupvm
  (testing "get-kausi-alkupvm luo oikean alkupäivämäärän ja loppupäivämäärän"
    (let [jakso1 {:koulutustoimija        "1.2.246.562.10.346830761110"
                  :tyopaikan_nimi         "Testi työpaikka"
                  :tyopaikan_ytunnus      "1234567-8"
                  :jakso_loppupvm         "2021-11-20"
                  :hankkimistapa_tyyppi   "koulutussopimus"}
          jakso2 {:koulutustoimija        "1.2.246.562.10.346830761110"
                  :tyopaikan_nimi         "Testi työpaikka"
                  :tyopaikan_ytunnus      "1234567-8"
                  :jakso_loppupvm         "2022-03-09"
                  :hankkimistapa_tyyppi   "koulutussopimus"}]
      (is (= "2021-07-01" (tpk/get-kausi-alkupvm jakso1)))
      (is (= "2022-01-01" (tpk/get-kausi-alkupvm jakso2)))
      (is (= "2021-12-31" (tpk/get-kausi-loppupvm jakso1)))
      (is (= "2022-06-30" (tpk/get-kausi-loppupvm jakso2))))))

(deftest test-create-nippu-id
  (testing "create-nippu-id luo nipun ID:n oikein"
    (let [jakso1 {:koulutustoimija        "1.2.246.562.10.346830761110"
                  :tyopaikan_nimi         "Testi työpaikka"
                  :tyopaikan_ytunnus      "1234567-8"
                  :jakso_loppupvm         "2021-11-20"
                  :hankkimistapa_tyyppi   "koulutussopimus"}]
      (is (= "testi_tyopaikka/1234567-8/1.2.246.562.10.346830761110/2021-07-01_2021-12-31"
             (tpk/create-nippu-id jakso1))))))

(deftest test-get-next-vastaamisajan-alkupvm-date
  (testing "get-next-vastaamisajan-alkupvm-date palauttaa oikean arvon"
    (let [jakso1 {:koulutustoimija        "1.2.246.562.10.346830761110"
                  :tyopaikan_nimi         "Testi työpaikka"
                  :tyopaikan_ytunnus      "1234567-8"
                  :jakso_loppupvm         "2021-11-20"
                  :hankkimistapa_tyyppi   "koulutussopimus"}
          jakso2 {:koulutustoimija        "1.2.246.562.10.346830761110"
                  :tyopaikan_nimi         "Testi työpaikka"
                  :tyopaikan_ytunnus      "1234567-8"
                  :jakso_loppupvm         "2022-02-02"
                  :hankkimistapa_tyyppi   "koulutussopimus"}]
      (is (= "2022-01-01"
             (str (tpk/get-next-vastaamisajan-alkupvm-date jakso1))))
      (is (= "2022-07-01"
             (str (tpk/get-next-vastaamisajan-alkupvm-date jakso2)))))))

(deftest test-create-nippu
  (testing "Varmistaa, että create-nippu luo niput oikein"
    (let [jakso {:koulutustoimija        "1.2.246.562.10.346830761110"
                 :tyopaikan_nimi         "Testi työpaikka"
                 :tyopaikan_ytunnus      "1234567-8"
                 :jakso_loppupvm         "2021-11-20"
                 :hankkimistapa_tyyppi   "koulutussopimus"}]
      (is (= (tpk/create-nippu jakso "abcde")
             {:nippu-id
              "testi_tyopaikka/1234567-8/1.2.246.562.10.346830761110/2021-07-01_2021-12-31"
              :tyopaikan-nimi               "Testi työpaikka"
              :tyopaikan-nimi-normalisoitu  "testi_tyopaikka"
              :vastaamisajan-alkupvm        "2022-01-01"
              :vastaamisajan-loppupvm       "2022-02-28"
              :tyopaikan-ytunnus            "1234567-8"
              :koulutustoimija-oid          "1.2.246.562.10.346830761110"
              :tiedonkeruu-alkupvm          "2021-07-01"
              :tiedonkeruu-loppupvm         "2021-12-31"
              :niputuspvm                   (str (t/today))
              :request-id                   "abcde"})))))
