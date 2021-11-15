(ns oph.heratepalvelu.jaksoHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.tep.jaksoHandler :as jh])
  (:import (java.time LocalDate)))

(deftest kesto-test
  (testing "Keston laskenta ottaa huomioon osa-aikaisuuden, opiskeluoikeuden väliaikaisen keskeytymisen ja lomat"
    (let [herate {:alkupvm "2021-06-01" :loppupvm "2021-06-30"}
          herate-osa-aikainen {:alkupvm "2021-06-01" :loppupvm "2021-06-30" :osa-aikaisuus 75}
          oo-tilat [{:alku "2021-05-01" :tila {:koodiarvo "lasna"}}]
          oo-tilat-kesk [{:alku "2021-06-15" :tila {:koodiarvo "lasna"}}
                         {:alku "2021-06-10" :tila {:koodiarvo "valiaikaisestikeskeytynyt"}}
                         {:alku "2021-05-01" :tila {:koodiarvo "lasna"}}]
          oo-tilat-kesk-loppuun-asti [{:alku "2021-06-10" :tila {:koodiarvo "valiaikaisestikeskeytynyt"}}
                                      {:alku "2021-05-01" :tila {:koodiarvo "lasna"}}]
          oo-tilat-loma [{:alku "2021-06-20" :tila {:koodiarvo "loma"}}
                         {:alku "2021-05-01" :tila {:koodiarvo "lasna"}}
                         {:alku "2021-06-25" :tila {:koodiarvo "lasna"}}]
          oo-tilat-kesk-loma [{:alku "2021-06-14" :tila {:koodiarvo "valiaikaisestikeskeytynyt"}}
                              {:alku "2021-05-01" :tila {:koodiarvo "loma"}}
                              {:alku "2021-06-20" :tila {:koodiarvo "loma"}}
                              {:alku "2021-06-02" :tila {:koodiarvo "lasna"}}
                              {:alku "2021-06-27" :tila {:koodiarvo "lasna"}}
                              {:alku "2021-06-16" :tila {:koodiarvo "lasna"}}]]
      (is (= 22 (jh/kesto herate oo-tilat)))
      (is (= 17 (jh/kesto herate-osa-aikainen oo-tilat)))
      (is (= 7  (jh/kesto herate oo-tilat-kesk-loppuun-asti)))
      (is (= 19 (jh/kesto herate oo-tilat-kesk)))
      (is (= 18 (jh/kesto herate oo-tilat-loma)))
      (is (= 14 (jh/kesto herate oo-tilat-kesk-loma)))
      (is (= 11 (jh/kesto herate-osa-aikainen oo-tilat-kesk-loma))))))

(deftest check-opiskeluoikeus-tila-test
  (testing "Opiskeluoikeuden tilan tarkastus. Keskeytetty opiskeluoikeus estää jakson käsittelyn.
  Jakson päättymispäivänä keskeytetty opiskeluoikeus ei estä jakson käsittelyä."
    (let [loppupvm "2021-09-07"
          opiskeluoikeus-lasna {:tila
                                {:opiskeluoikeusjaksot [{:alku "2021-06-20" :tila {:koodiarvo "loma"}}
                                                        {:alku "2021-05-01" :tila {:koodiarvo "lasna"}}
                                                        {:alku "2021-06-25" :tila {:koodiarvo "lasna"}}]}}
          opiskeluoikeus-eronnut-samana-paivana {:tila
                                                {:opiskeluoikeusjaksot [{:alku "2021-06-20" :tila {:koodiarvo "loma"}}
                                                                        {:alku "2021-05-01" :tila {:koodiarvo "lasna"}}
                                                                        {:alku "2021-09-07" :tila {:koodiarvo "eronnut"}}]}}
          opiskeluoikeus-eronnut-tulevaisuudessa {:tila
                                                       {:opiskeluoikeusjaksot [{:alku "2021-06-20" :tila {:koodiarvo "loma"}}
                                                                               {:alku "2021-05-01" :tila {:koodiarvo "lasna"}}
                                                                               {:alku "2021-09-08" :tila {:koodiarvo "eronnut"}}]}}
          opiskeluoikeus-eronnut-paivaa-aiemmin {:tila
                                                      {:opiskeluoikeusjaksot [{:alku "2021-06-20" :tila {:koodiarvo "loma"}}
                                                                              {:alku "2021-05-01" :tila {:koodiarvo "lasna"}}
                                                                              {:alku "2021-09-06" :tila {:koodiarvo "eronnut"}}]}}]
      (is (= true (jh/check-opiskeluoikeus-tila opiskeluoikeus-lasna loppupvm)))
      (is (= true (jh/check-opiskeluoikeus-tila opiskeluoikeus-eronnut-samana-paivana loppupvm)))
      (is (= true (jh/check-opiskeluoikeus-tila opiskeluoikeus-eronnut-tulevaisuudessa loppupvm)))
      (is (nil? (jh/check-opiskeluoikeus-tila opiskeluoikeus-eronnut-paivaa-aiemmin loppupvm))))))

(deftest check-sort-process-keskeytymisajanjaksot-test
  (testing "sort-process-keskeytymisajanjaksot test"
    (let [herate1 {:keskeytymisajanjaksot [{:alku "2021-08-08" :loppu "2021-08-10"}
                                          {:alku "2021-08-01" :loppu "2021-08-04"}]}
          herate2 {:keskeytymisajanjaksot [{:alku "2021-08-08"}
                                          {:alku "2021-08-01" :loppu "2021-08-04"}]}
          expected1 [{:alku (LocalDate/parse "2021-08-01") :loppu (LocalDate/parse "2021-08-04")}
                    {:alku (LocalDate/parse "2021-08-08") :loppu (LocalDate/parse "2021-08-10")}] 
          expected2 [{:alku (LocalDate/parse "2021-08-01") :loppu (LocalDate/parse "2021-08-04")}
                    {:alku (LocalDate/parse "2021-08-08") :loppu nil}]]
      (is (= expected1 (jh/sort-process-keskeytymisajanjaksot herate1)))
      (is (= expected2 (jh/sort-process-keskeytymisajanjaksot herate2))))))

(deftest check-not-fully-keskeytynyt-test
  (testing "check-not-fully-keskeytynyt test"
    (let [herate1 {:keskeytymisajanjaksot [{:alku "2021-08-08" :loppu "2021-08-10"}
                                           {:alku "2021-08-01" :loppu "2021-08-04"}]
                   :loppupvm "2021-08-09"}
          herate2 {:keskeytymisajanjaksot [{:alku "2021-08-08" :loppu "2021-08-10"}
                                           {:alku "2021-08-01" :loppu "2021-08-04"}]
                   :loppupvm "2021-08-11"}
          herate3 {}
          herate4 {:keskeytymisajanjaksot [{:alku "2021-08-08"}]}]
      (is (not (jh/check-not-fully-keskeytynyt herate1)))
      (is (true? (jh/check-not-fully-keskeytynyt herate2)))
      (is (true? (jh/check-not-fully-keskeytynyt herate3)))
      (is (true? (jh/check-not-fully-keskeytynyt herate4))))))

(deftest check-open-keskeytymisajanjakso-test
  (testing "check-open-keskeytymisajanjakso test"
    (let [herate1 {:keskeytymisajanjaksot [{:alku "2021-08-08" :loppu "2021-08-10"}
                                           {:alku "2021-08-01" :loppu "2021-08-04"}]}
          herate2 {}
          herate3 {:keskeytymisajanjaksot [{:alku "2021-08-08"}]}]
      (is (not (jh/check-open-keskeytymisajanjakso herate1)))
      (is (not (jh/check-open-keskeytymisajanjakso herate2)))
      (is (true? (jh/check-open-keskeytymisajanjakso herate3))))))
