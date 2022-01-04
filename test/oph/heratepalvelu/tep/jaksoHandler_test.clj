(ns oph.heratepalvelu.tep.jaksoHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.tep.jaksoHandler :as jh]
            [oph.heratepalvelu.test-util :as tu])
  (:import (clojure.lang ExceptionInfo)
           (java.time LocalDate)))

(use-fixtures :each tu/clear-logs-before-test)

(deftest test-tep-herate-checker
  (testing "Varmista, että tep-herate-checker vaatii oikean scheman"
    (let [good1 {:tyyppi "aloittaneet"
                 :alkupvm "2022-01-01"
                 :loppupvm "2022-02-02"
                 :hoks-id 1234
                 :opiskeluoikeus-oid "123-456-789"
                 :oppija-oid "234-567-891"
                 :hankkimistapa-id 789
                 :hankkimistapa-tyyppi "01"
                 :tutkinnonosa-id 67
                 :tutkinnonosa-koodi "asdf"
                 :tutkinnonosa-nimi "A S D F"
                 :tyopaikan-nimi "ABCD"
                 :tyopaikan-ytunnus "123456-7"
                 :tyopaikkaohjaaja-email "esa@esimerkki.fi"
                 :tyopaikkaohjaaja-nimi "Esa Esimerkki"}
          good2 (assoc good1 :keskeytymisajanjaksot [])
          good3 (assoc good1 :keskeytymisajanjaksot [{:alku "2022-01-05"}])
          bad1 (assoc good1 :keskeytymisajanjaksot [{}])
          bad2 (assoc good1 :tyyppi "")]
      (is (nil? (jh/tep-herate-checker good1)))
      (is (nil? (jh/tep-herate-checker good2)))
      (is (nil? (jh/tep-herate-checker good3)))
      (is (some? (jh/tep-herate-checker bad1)))
      (is (some? (jh/tep-herate-checker bad2))))))

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

(defn- mock-check-duplicate-hankkimistapa-get-item [query-params table]
  (when (and (= :n (first (:hankkimistapa_id query-params)))
             (= "jaksotunnus-table-name" table))
    (if (= 123 (second (:hankkimistapa_id query-params)))
      []
      {:hankkimistapa_id (second (:hankkimistapa_id query-params))})))

(deftest test-check-duplicate-hankkimistapa
  (testing "Varmista, että check-duplicate-hankkimistapa toimii oikein"
    (with-redefs [clojure.tools.logging/log* tu/mock-log*
                  environ.core/env {:jaksotunnus-table "jaksotunnus-table-name"}
                  oph.heratepalvelu.db.dynamodb/get-item
                  mock-check-duplicate-hankkimistapa-get-item]
      (is (nil? (jh/check-duplicate-hankkimistapa 456)))
      (is (true?
            (tu/logs-contain?
              {:level :warn
               :message "Osaamisenhankkimistapa id 456 on jo käsitelty."})))
      (is (true? (jh/check-duplicate-hankkimistapa 123))))))

(defn- mock-check-duplicate-tunnus-query-items [query-params options table]
  (when (and (= :eq (first (:tunnus query-params)))
             (= :s (first (second (:tunnus query-params))))
             (= "ABCDEF" (second (second (:tunnus query-params))))
             (= "uniikkiusIndex" (:index options))
             (= "jaksotunnus-table-name" table))
    {:items [{:tunnus "ABCDEF"}]}))

(deftest test-check-duplicate-tunnus
  (testing "Varmista, että check-duplicate-tunnus tunnistaa duplikaattoja"
    (with-redefs [environ.core/env {:jaksotunnus-table "jaksotunnus-table-name"}
                  oph.heratepalvelu.db.dynamodb/query-items
                  mock-check-duplicate-tunnus-query-items]
      (is (true? (jh/check-duplicate-tunnus "ABCDDD")))
      (is (thrown-with-msg? ExceptionInfo
                            #"Tunnus ABCDEF on jo käytössä"
                            (jh/check-duplicate-tunnus "ABCDEF"))))))

(def mock-save-to-tables-put-item-results (atom {}))

(defn- mock-save-to-tables-put-item [data options table]
  (if (= table "jaksotunnus-table-name")
    (when (= "attribute_not_exists(hankkimistapa_id)" (:cond-expr options))
      (reset! mock-save-to-tables-put-item-results
              (assoc @mock-save-to-tables-put-item-results
                     :jaksotunnus-table-data
                     data)))
    (when (and (= table "nippu-table-name") (= {} options))
      (reset! mock-save-to-tables-put-item-results
              (assoc @mock-save-to-tables-put-item-results
                     :nippu-table-data
                     data)))))

(deftest test-save-to-tables
  (testing "Varmista, että jaksotunnus- ja nipputiedot tallennetaan oikein"
    (with-redefs
      [environ.core/env {:jaksotunnus-table "jaksotunnus-table-name"
                         :nippu-table "nippu-table-name"}
       oph.heratepalvelu.db.dynamodb/put-item mock-save-to-tables-put-item]
      (let [jaksotunnus-table-data {:contents "jaksotunnus-table-data"}
            nippu-table-data {:contents "nippu-table-data"}
            results {:jaksotunnus-table-data jaksotunnus-table-data
                     :nippu-table-data nippu-table-data}]
        (jh/save-to-tables jaksotunnus-table-data nippu-table-data)
        (is (= results @mock-save-to-tables-put-item-results))))))
