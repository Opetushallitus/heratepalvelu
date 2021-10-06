(ns oph.heratepalvelu.jaksoHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.tep.jaksoHandler :as jh]))

(deftest kesto-test
  (testing "Keston laskenta ottaa huomioon osa-aikaisuuden, opiskeluoikeuden väliaikaisen keskeytymisen ja lomat"
    (let [herate {:alkupvm "2021-06-01" :loppupvm "2021-06-30"}
          herate-osa-aikainen {:alkupvm "2021-06-01" :loppupvm "2021-06-30" :osa-aikaisuus 75}
          oo-tilat [{:alku "2021-05-01" :tila {:koodiarvo "lasna"}}]
          oo-tilat-kesk [{:alku "2021-06-15" :tila {:koodiarvo "lasna"}}
                         {:alku "2021-06-10" :tila {:koodiarvo "valiaikaisestikeskeytynyt"}}
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
          opiskeluoikeus-kesk-eronnut-tulevaisuudessa {:tila
                                                       {:opiskeluoikeusjaksot [{:alku "2021-06-20" :tila {:koodiarvo "loma"}}
                                                                               {:alku "2021-05-01" :tila {:koodiarvo "lasna"}}
                                                                               {:alku "2021-09-08" :tila {:koodiarvo "eronnut"}}]}}
          opiskeluoikeus-kesk-eronnut-paivaa-aiemmin {:tila
                                                      {:opiskeluoikeusjaksot [{:alku "2021-06-20" :tila {:koodiarvo "loma"}}
                                                                              {:alku "2021-05-01" :tila {:koodiarvo "lasna"}}
                                                                              {:alku "2021-09-06" :tila {:koodiarvo "eronnut"}}]}}]
      (is (= true (jh/check-opiskeluoikeus-tila opiskeluoikeus-lasna loppupvm)))
      (is (= true (jh/check-opiskeluoikeus-tila opiskeluoikeus-eronnut-samana-paivana loppupvm)))
      (is (= true (jh/check-opiskeluoikeus-tila opiskeluoikeus-kesk-eronnut-tulevaisuudessa loppupvm)))
      (is (nil? (jh/check-opiskeluoikeus-tila opiskeluoikeus-kesk-eronnut-paivaa-aiemmin loppupvm))))))
