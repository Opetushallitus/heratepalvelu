(ns oph.heratepalvelu.util.date-test
  (:require
   [clojure.test :refer [deftest is testing]]
   [oph.heratepalvelu.util.date :as d])
  (:import java.time.LocalDate))

(def test-dates
  (map #(apply (fn [^Integer y ^Integer m ^Integer d]
                 (LocalDate/of y m d)) %)
       (partition 3 [2023 1 26
                     2023 1 27
                     2023 1 28
                     2023 1 29
                     2023 1 30
                     2023 1 31
                     2023 2 1
                     2023 2 2
                     2023 2 3
                     2023 2 4])))

(deftest test-range
  (testing "`range` rakentaa onnistuneesti päivämääräsekvenssin, johon kuuluvat
            sekä parametreina annetut alku- ja loppupäivämäärät."
    (is (= (d/range (LocalDate/of 2023 1 26) (LocalDate/of 2023 2 04))
           test-dates))))
