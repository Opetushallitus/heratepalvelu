(ns oph.heratepalvelu.tpk.tpkCommon-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.tpk.tpkCommon :as tpkc])
  (:import (java.time LocalDate)))

(deftest test-get-kausi-alkupvm-loppupvm
  (testing "get-kausi-alkupvm/-loppupvm luo oikean alku- ja loppupäivämäärän"
    (let [pvm1 (LocalDate/of 2021 11 20)
          pvm2 (LocalDate/of 2022 3 9)]
      (is (= (LocalDate/of 2021 7 1) (tpkc/get-kausi-alkupvm pvm1)))
      (is (= (LocalDate/of 2022 1 1) (tpkc/get-kausi-alkupvm pvm2)))
      (is (= (LocalDate/of 2021 12 31) (tpkc/get-kausi-loppupvm pvm1)))
      (is (= (LocalDate/of 2022 6 30) (tpkc/get-kausi-loppupvm pvm2))))))

(deftest test-get-current-kausi-alkupvm-loppupvm
  (testing "get-current-kausi-alkupvm ja get-current-kausi-loppupvm"
    (with-redefs [oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2021 9 9))]
      (is (= (LocalDate/of 2021 7 1) (tpkc/get-current-kausi-alkupvm)))
      (is (= (LocalDate/of 2021 12 31) (tpkc/get-current-kausi-loppupvm))))
    (with-redefs [oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 2 2))]
      (is (= (LocalDate/of 2021 7 1) (tpkc/get-current-kausi-alkupvm)))
      (is (= (LocalDate/of 2021 12 31) (tpkc/get-current-kausi-loppupvm))))))
