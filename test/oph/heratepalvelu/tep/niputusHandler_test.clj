(ns oph.heratepalvelu.tep.niputusHandler-test
  (:require [clojure.test :refer [are deftest is testing]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.tep.niputusHandler :as nh]
            [oph.heratepalvelu.external.koski :as koski]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(deftest test-not-in-keskeytymisajanjakso?
  (testing "Varmistaa, että not-in-keskeytymisajanjakso? toimii oikein."
    (let [kjaksot [{:loppu (LocalDate/of 2022 4 1)}
                   {:alku  (LocalDate/of 2022 4 4)
                    :loppu (LocalDate/of 2022 4 6)}
                   {:alku  (LocalDate/of 2022 4 8)}]]
      (is (true? (nh/not-in-keskeytymisajanjakso? (LocalDate/of 2022 4 4) [])))
      (is (true? (nh/not-in-keskeytymisajanjakso? (LocalDate/of 2022 4 2)
                                                  kjaksot)))
      (is (true? (nh/not-in-keskeytymisajanjakso? (LocalDate/of 2022 4 7)
                                                  kjaksot)))
      (is (false? (nh/not-in-keskeytymisajanjakso? (LocalDate/of 2022 3 30)
                                                   kjaksot)))
      (is (false? (nh/not-in-keskeytymisajanjakso? (LocalDate/of 2022 4 4)
                                                   kjaksot)))
      (is (false? (nh/not-in-keskeytymisajanjakso? (LocalDate/of 2022 4 6)
                                                   kjaksot)))
      (is (false? (nh/not-in-keskeytymisajanjakso? (LocalDate/of 2022 4 10)
                                                   kjaksot))))))

(deftest test-filtered-jakso-days
  (testing "Varmistaa, että filtered-jakso-days toimii oikein."
    (let [test-jakso1 {:jakso_alkupvm "2022-04-27" :jakso_loppupvm "2022-05-02"}
          test-jakso2 {:jakso_alkupvm "2022-04-24" :jakso_loppupvm "2022-04-30"}
          days1 (seq [(LocalDate/of 2022 4 27)
                      (LocalDate/of 2022 4 28)
                      (LocalDate/of 2022 4 29)
                      (LocalDate/of 2022 5 2)])
          days2 (seq [(LocalDate/of 2022 4 25)
                      (LocalDate/of 2022 4 26)
                      (LocalDate/of 2022 4 27)
                      (LocalDate/of 2022 4 28)
                      (LocalDate/of 2022 4 29)])]
      (is (= (nh/filtered-jakso-days test-jakso1) days1))
      (is (= (nh/filtered-jakso-days test-jakso2) days2)))))

(deftest test-add-to-jaksot-by-day
  (testing "Varmistaa, että add-to-jaksot-by-day toimii oikein."
    (let [opiskeluoikeus {:tila
                          {:opiskeluoikeusjaksot
                           [{:alku "2022-01-01" :tila {:koodiarvo "lasna"}}
                            {:alku "2022-01-17" :tila {:koodiarvo "loma"}}
                            {:alku "2022-01-19" :tila {:koodiarvo "lasna"}}
                            {:alku "2022-01-21"
                             :tila {:koodiarvo "valiaikaisestikeskeytynyt"}}
                            {:alku "2022-02-25" :tila {:koodiarvo "lasna"}}]}}
          existing-jakso {:jakso_alkupvm "2022-01-10"
                          :jakso_loppupvm "2022-01-16"}
          jaksot-by-day {(LocalDate/of 2022 1 10) (seq [existing-jakso])
                         (LocalDate/of 2022 1 11) (seq [existing-jakso])
                         (LocalDate/of 2022 1 12) (seq [existing-jakso])
                         (LocalDate/of 2022 1 13) (seq [existing-jakso])
                         (LocalDate/of 2022 1 14) (seq [existing-jakso])}
          jakso {:jakso_alkupvm "2022-01-09"
                 :jakso_loppupvm "2022-01-24"
                 :keskeytymisajanjaksot [{:alku "2022-01-12"
                                          :loppu "2022-01-12"}]
                 :opiskeluoikeus_oid "asdf"}
          results {(LocalDate/of 2022 1 10) (seq [jakso existing-jakso])
                   (LocalDate/of 2022 1 11) (seq [jakso existing-jakso])
                   (LocalDate/of 2022 1 12) (seq [existing-jakso])
                   (LocalDate/of 2022 1 13) (seq [jakso existing-jakso])
                   (LocalDate/of 2022 1 14) (seq [jakso existing-jakso])
                   (LocalDate/of 2022 1 19) (seq [jakso])
                   (LocalDate/of 2022 1 20) (seq [jakso])}]
      (is (= (nh/add-to-jaksot-by-day jaksot-by-day jakso opiskeluoikeus)
             results)))))

(deftest test-add-to-jaksot-by-day-new
  (testing "Varmistaa, että add-to-jaksot-by-day-new toimii oikein."
    (let [opiskeluoikeus {:tila
                          {:opiskeluoikeusjaksot
                           [{:alku "2022-01-01" :tila {:koodiarvo "lasna"}}
                            {:alku "2022-01-17" :tila {:koodiarvo "loma"}}
                            {:alku "2022-01-19" :tila {:koodiarvo "lasna"}}
                            {:alku "2022-01-21"
                             :tila {:koodiarvo "valiaikaisestikeskeytynyt"}}
                            {:alku "2022-02-25" :tila {:koodiarvo "lasna"}}]}}
          existing-jakso {:jakso_alkupvm "2022-01-10"
                          :jakso_loppupvm "2022-01-16"}
          jaksot-by-day {(LocalDate/of 2022 1 10) (seq [existing-jakso])
                         (LocalDate/of 2022 1 11) (seq [existing-jakso])
                         (LocalDate/of 2022 1 12) (seq [existing-jakso])
                         (LocalDate/of 2022 1 13) (seq [existing-jakso])
                         (LocalDate/of 2022 1 14) (seq [existing-jakso])
                         (LocalDate/of 2022 1 15) (seq [existing-jakso])
                         (LocalDate/of 2022 1 16) (seq [existing-jakso])}
          jakso {:jakso_alkupvm "2022-01-09"
                 :jakso_loppupvm "2022-01-28"
                 :keskeytymisajanjaksot [{:alku "2022-01-12"
                                          :loppu "2022-01-12"}]
                 :opiskeluoikeus_oid "asdf"}
          expected {(LocalDate/of 2022 1  9) (seq [jakso])
                    (LocalDate/of 2022 1 10) (seq [jakso existing-jakso])
                    (LocalDate/of 2022 1 11) (seq [jakso existing-jakso])
                    (LocalDate/of 2022 1 12) (seq [existing-jakso])
                    (LocalDate/of 2022 1 13) (seq [jakso existing-jakso])
                    (LocalDate/of 2022 1 14) (seq [jakso existing-jakso])
                    (LocalDate/of 2022 1 15) (seq [jakso existing-jakso])
                    (LocalDate/of 2022 1 16) (seq [jakso existing-jakso])
                    (LocalDate/of 2022 1 17) (seq [jakso])
                    (LocalDate/of 2022 1 18) (seq [jakso])
                    (LocalDate/of 2022 1 19) (seq [jakso])
                    (LocalDate/of 2022 1 20) (seq [jakso])}]
      (is (= expected (nh/add-to-jaksot-by-day-new
                        jaksot-by-day jakso opiskeluoikeus))))))

(deftest test-handle-one-day
  (testing "Varmistaa, että handle-one-day toimii oikein."
    (let [jaksot (seq [{:hankkimistapa_id 1 :osa_aikaisuus 100}
                       {:hankkimistapa_id 2 :osa_aikaisuus 50}
                       {:hankkimistapa_id 3 :osa_aikaisuus 10}
                       {:hankkimistapa_id 4}])
          results {1 0.25
                   2 0.125
                   3 0.025
                   4 0.25}]
      (is (= (nh/handle-one-day jaksot) results)))))

(def get-tyoelamajaksot-active-between-call-params (atom {}))
(def mock-get-opiskeluoikeus-catch-404-count (atom 0))

(defn- do-rounding [values]
  ; FIXME: map-values
  (reduce-kv #(assoc %1 %2 (/ (Math/round (* %3 1000.0)) 1000.0)) {} values))

(defn- mock-get-opiskeluoikeus-catch-404 [oo-oid]
  (swap! mock-get-opiskeluoikeus-catch-404-count inc)
  (cond
    (= oo-oid "1.2.3.1") {:tila
                          {:opiskeluoikeusjaksot
                           [{:alku "2022-01-01" :tila {:koodiarvo "lasna"}}]}}
    (= oo-oid "1.2.3.2") {:tila
                          {:opiskeluoikeusjaksot
                           [{:alku "2022-01-01" :tila {:koodiarvo "lasna"}}]}}
    (= oo-oid "1.2.3.4") {:tila
                          {:opiskeluoikeusjaksot
                           [{:alku "2022-01-01" :tila {:koodiarvo "lasna"}}
                            {:alku "2022-01-30" :tila {:koodiarvo "loma"}}
                            {:alku "2022-02-10" :tila {:koodiarvo "lasna"}}]}}
    (= oo-oid "1.2.3.5") {:tila
                          {:opiskeluoikeusjaksot
                           [{:alku "2022-01-01" :tila {:koodiarvo "lasna"}}
                            {:alku "2022-02-02"
                             :tila {:koodiarvo "valiaikaisestikeskeytynyt"}}]}}
    (= oo-oid "1.2.3.8") {:tila
                          {:opiskeluoikeusjaksot
                           [{:alku "2020-01-01", :tila {:koodiarvo "lasna"}}]}}
    (= oo-oid "1.2.3.7") {:tila
                          {:opiskeluoikeusjaksot
                           [{:alku "2020-04-06",
                             :tila {:koodiarvo "lasna"}}]}}
    (= oo-oid "1.2.3.9") {:tila
                          {:opiskeluoikeusjaksot
                           [{:alku "2022-08-25",
                             :tila {:koodiarvo "lasna"}}]}}
    (= oo-oid "1.2.3.6") {:tila
                          {:opiskeluoikeusjaksot
                           [{:alku "2020-03-05",
                             :tila {:koodiarvo "lasna"}}]}}
    (= oo-oid "1.2.3.10") {:tila
                           {:opiskeluoikeusjaksot
                            [{:alku "2020-10-27",
                              :tila {:koodiarvo "lasna"}}]}}
    :else nil))

(defn- get-test-opiskeluoikeudet
  [oids]
  (reduce #(assoc % %2 (mock-get-opiskeluoikeus-catch-404 %2)) {} oids))

(deftest test-get-and-memoize-opiskeluoikeudet
  (with-redefs
   [oph.heratepalvelu.external.koski/get-opiskeluoikeus-catch-404!
    mock-get-opiskeluoikeus-catch-404]
    (testing "Funktio palauttaa samat opiskeluoikeudet kun
             `get-test-opiskeluoikeudet` ja `get-opiskeluoikeus-catch-404!`
             -kutsujen määrä on yhtä suuri kuin uniikkien haettavien
             opiskeluoikeuksien määrä."
      (are [oids]
           (= (do (reset! mock-get-opiskeluoikeus-catch-404-count 0)
                  [(nh/get-and-memoize-opiskeluoikeudet!
                   (map #(assoc {} :opiskeluoikeus_oid %) oids))
                   @mock-get-opiskeluoikeus-catch-404-count])
              [(get-test-opiskeluoikeudet oids) (count (distinct oids))])
        []
        ["1.2.3.a"]
        ["1.2.3.7" "1.2.3.9" "1.2.3.10.onnea.matkaan"]
        ["1.2.3.a" "1.2.3.a" "1.2.3.8" "1.2.3.b" "1.2.3.8" "1.2.3.b"]
        (map str (repeat "1.2.3.") [9 10 6 6 7 10 6 8 10])))))


(deftest test-get-opiskeluoikeusjaksot
  (testing "Funktio hakee onnistuneesti opiskeluoikeuden opiskeluoikeusjaksot."
    (are [oo-oid expected] (= (nh/get-opiskeluoikeusjaksot
                               (mock-get-opiskeluoikeus-catch-404 oo-oid))
                              expected)
      "1.2.3.4" (map c/alku-and-loppu-to-localdate
                     [{:tila {:koodiarvo "lasna"} :alku "2022-01-01" :loppu "2022-01-29"}
                      {:tila {:koodiarvo "loma"} :alku "2022-01-30" :loppu "2022-02-09"}
                      {:tila {:koodiarvo "lasna"} :alku "2022-02-10"}])
      "1.2.3.5" (map c/alku-and-loppu-to-localdate
                     [{:tila {:koodiarvo "lasna"}
                       :alku "2022-01-01"
                       :loppu "2022-02-01"}
                      {:tila {:koodiarvo "valiaikaisestikeskeytynyt"}
                       :alku "2022-02-02"}]))))

(def jaksot-1 [{:hankkimistapa_id 1
                :oppija_oid "4.4.4.4"
                :osa_aikaisuus 100
                :jakso_alkupvm "2022-01-03"
                :jakso_loppupvm "2022-02-05"
                :keskeytymisajanjaksot []
                :opiskeluoikeus_oid "1.2.3.1"}
               {:hankkimistapa_id 2
                :oppija_oid "4.4.4.4"
                :osa_aikaisuus 50
                :jakso_alkupvm "2022-02-07"
                :jakso_loppupvm "2022-04-04"
                :opiskeluoikeus_oid "1.2.3.2"}
               {:hankkimistapa_id 3
                :oppija_oid "4.4.4.4"
                :osa_aikaisuus 0
                :jakso_alkupvm "2022-01-31"
                :jakso_loppupvm "2022-02-20"
                :keskeytymisajanjaksot [{:alku "2022-02-16"
                                         :loppu "2022-02-18"}]
                :opiskeluoikeus_oid "1.2.3.3"}
               {:hankkimistapa_id 4
                :oppija_oid "4.4.4.4"
                :jakso_alkupvm "2022-01-17"
                :jakso_loppupvm "2022-02-20"
                :opiskeluoikeus_oid "1.2.3.4"}
               {:hankkimistapa_id 5
                :oppija_oid "4.4.4.4"
                :jakso_alkupvm "2022-01-01"
                :jakso_loppupvm "2022-04-01"
                :opiskeluoikeus_oid "1.2.3.5"}])

(def jaksot-2 [{:opiskeluoikeus_oid "1.2.3.6",
                :hankkimistapa_id 9,
                :jakso_alkupvm "2021-07-03",
                :jakso_loppupvm "2021-08-26",
                :osa_aikaisuus nil,
                :tyyppi "hato",
                :keskeytymisajanjaksot []},
               {:opiskeluoikeus_oid "1.2.3.7",
                :hankkimistapa_id 14,
                :jakso_alkupvm "2021-09-01",
                :jakso_loppupvm "2024-12-15",
                :osa_aikaisuus nil,
                :tyyppi "hato",
                :keskeytymisajanjaksot []}
               {:opiskeluoikeus_oid "1.2.3.7",
                :hankkimistapa_id 5,
                :jakso_alkupvm "2021-09-01",
                :jakso_loppupvm "2023-12-15",
                :osa_aikaisuus nil,
                :tyyppi "hato",
                :keskeytymisajanjaksot []}
               {:opiskeluoikeus_oid "1.2.3.7",
                :hankkimistapa_id 11,
                :jakso_alkupvm "2021-09-03",
                :jakso_loppupvm "2021-09-30",
                :osa_aikaisuus nil,
                :tyyppi "hato",
                :keskeytymisajanjaksot []}
               {:opiskeluoikeus_oid "1.2.3.7",
                :hankkimistapa_id 10,
                :jakso_alkupvm "2021-08-20",
                :jakso_loppupvm "2021-09-10",
                :osa_aikaisuus nil,
                :tyyppi "hato",
                :keskeytymisajanjaksot [{:id 1191,
                                         :osaamisen-hankkimistapa-id 10,
                                         :alku "2021-09-21"}]}
               {:opiskeluoikeus_oid "1.2.3.7",
                :hankkimistapa_id 12,
                :jakso_alkupvm "2021-11-12",
                :jakso_loppupvm "2021-12-15",
                :osa_aikaisuus nil,
                :tyyppi "hato",
                :keskeytymisajanjaksot [{:id 1192,
                                         :osaamisen-hankkimistapa-id 12,
                                         :alku "2021-11-18",
                                         :loppu "2021-11-25"}]}
               {:opiskeluoikeus_oid "1.2.3.8",
                :hankkimistapa_id 13,
                :jakso_alkupvm "2021-09-06",
                :jakso_loppupvm "2021-10-19",
                :osa_aikaisuus 60,
                :tyyppi "hato",
                :keskeytymisajanjaksot [{:id 1029,
                                         :osaamisen-hankkimistapa-id 13,
                                         :alku "2021-10-13"}]}
               {:opiskeluoikeus_oid "1.2.3.8",
                :hankkimistapa_id 3,
                :jakso_alkupvm "2021-10-11",
                :jakso_loppupvm "2021-10-21",
                :osa_aikaisuus nil,
                :tyyppi "hato",
                :keskeytymisajanjaksot [{:id 1030,
                                         :osaamisen-hankkimistapa-id 3,
                                         :alku "2021-10-13",
                                         :loppu "2021-10-14"}]}
               {:opiskeluoikeus_oid "1.2.3.8",
                :hankkimistapa_id 6,
                :jakso_alkupvm "2021-10-11",
                :jakso_loppupvm "2021-10-21",
                :osa_aikaisuus nil,
                :tyyppi "hato",
                :keskeytymisajanjaksot [{:id 1031,
                                         :osaamisen-hankkimistapa-id 6,
                                         :alku "2021-10-13"}]}
               {:opiskeluoikeus_oid "1.2.3.8",
                :hankkimistapa_id 1,
                :jakso_alkupvm "2021-10-22",
                :jakso_loppupvm "2021-10-25",
                :osa_aikaisuus 40,
                :tyyppi "hato",
                :keskeytymisajanjaksot [{:id 1032,
                                         :osaamisen-hankkimistapa-id 1,
                                         :alku "2022-02-11",
                                         :loppu "3022-03-01"}]}
               {:opiskeluoikeus_oid "1.2.3.8",
                :hankkimistapa_id 15,
                :jakso_alkupvm "2021-10-04",
                :jakso_loppupvm "2021-10-19",
                :osa_aikaisuus nil,
                :tyyppi "hato",
                :keskeytymisajanjaksot []}
               {:opiskeluoikeus_oid "1.2.3.8",
                :hankkimistapa_id 8,
                :jakso_alkupvm "2021-10-15",
                :jakso_loppupvm "2021-12-11",
                :osa_aikaisuus nil,
                :tyyppi "hato",
                :keskeytymisajanjaksot []}
               {:opiskeluoikeus_oid "1.2.3.8",
                :hankkimistapa_id 2,
                :jakso_alkupvm "2021-09-01",
                :jakso_loppupvm "2021-12-15",
                :osa_aikaisuus 80,
                :tyyppi "hato",
                :keskeytymisajanjaksot [{:id 1033,
                                         :osaamisen-hankkimistapa-id 2,
                                         :alku "2021-10-22",
                                         :loppu "2021-10-30"}]}
               {:opiskeluoikeus_oid "1.2.3.8",
                :hankkimistapa_id 4,
                :jakso_alkupvm "2021-05-15",
                :jakso_loppupvm "2021-12-01",
                :osa_aikaisuus nil,
                :tyyppi "hato",
                :keskeytymisajanjaksot []}
               {:opiskeluoikeus_oid "1.2.3.9",
                :hankkimistapa_id 16,
                :jakso_alkupvm "2021-02-01",
                :jakso_loppupvm "2021-08-30",
                :osa_aikaisuus nil,
                :tyyppi "hato",
                :keskeytymisajanjaksot [{:id 1034,
                                         :osaamisen-hankkimistapa-id 16,
                                         :alku "2021-06-15"}]}
               {:opiskeluoikeus_oid "1.2.3.10",
                :hankkimistapa_id 7,
                :jakso_alkupvm "2021-01-15",
                :jakso_loppupvm "2022-04-29",
                :osa_aikaisuus 50,
                :tyyppi "hato",
                :keskeytymisajanjaksot []}
               {:opiskeluoikeus_oid "1.2.3.10.onnea.etsintaan",
                :hankkimistapa_id 17,
                :jakso_alkupvm "2021-08-27",
                :jakso_loppupvm "2022-04-29",
                :osa_aikaisuus 100,
                :tyyppi "hato",
                :keskeytymisajanjaksot []}])

(deftest test-in-jakso?
  (testing "Funktio palauttaa"
    (let [jakso {:alku  (LocalDate/parse "2022-07-08")
                 :loppu (LocalDate/parse "2022-12-20")}
          jakso-2 {:alku (LocalDate/parse "2022-05-01")}]
      (testing "`true`, kun päivämäärä on"
        (testing "suljetulla aikavälillä"
          (are [pvm] (nh/in-jakso? (LocalDate/parse pvm) jakso)
            "2022-07-08" "2022-12-20" "2022-10-18" "2022-08-31"))
        (testing "puoliavoimella aikavälillä"
          (are [pvm] (nh/in-jakso? (LocalDate/parse pvm) jakso-2)
            "2022-05-01" "2022-05-02" "2022-10-18" "2024-01-01")))
      (testing "`false`, kun päivämäärä on"
        (testing "suljetun aikavälin ulkopuolella"
          (are [pvm] (not (nh/in-jakso? (LocalDate/parse pvm) jakso))
            "2022-07-07" "2022-12-21" "2021-07-08" "2022-12-30"))
        (testing "puoliavoimen aikavälin ulkopuolella"
          (are [pvm] (not (nh/in-jakso? (LocalDate/parse pvm) jakso-2))
            "2022-04-30" "2022-01-01" "2021-12-30" "2020-06-05"))))))

(deftest test-jakso-active?
  (let [jakso-1 {:alku (LocalDate/parse "2022-01-03")
                 :loppu (LocalDate/parse "2022-05-05")
                 :keskeytymisajanjaksot [{:alku "2022-02-20" :loppu "2022-03-03"}
                                         {:alku "2022-04-08"}]}
        jakso-2 {:alku (LocalDate/parse "2022-01-01")}
        opiskeluoikeus-1 (mock-get-opiskeluoikeus-catch-404 "1.2.3.4")
        opiskeluoikeus-2 (mock-get-opiskeluoikeus-catch-404 "1.2.3.5")]
    (testing "Funktio palauttaa"
      (testing (str "`true`, kun päivämäärä on jakson sisällä, eikä se kuulu "
                    "mihinkään keskeytymisajanjaksoon.")
        (are [pvm] (nh/jakso-active? jakso-1 opiskeluoikeus-1 (LocalDate/parse pvm))
          "2022-01-03" "2022-02-19" "2022-03-04" "2022-04-07" "2022-02-10")
        (are [pvm] (nh/jakso-active? jakso-2 opiskeluoikeus-2 (LocalDate/parse pvm))
          "2022-01-01" "2022-01-21" "2022-02-01"))
      (testing "`false`,"
        (testing "kun opiskeluoikeus on `nil`."
          (are [pvm] (not (nh/jakso-active? jakso-1 nil (LocalDate/parse pvm)))
            "2022-01-03" "2022-02-19" "2022-03-04" "2022-04-07" "2022-02-10"))
        (testing "kun päivämäärä"
          (testing "sisältyy keskeytymisajanjaksoon."
            (are [pvm] (not (nh/jakso-active? jakso-1 opiskeluoikeus-1 (LocalDate/parse pvm)))
              "2022-02-20" "2022-03-01" "2022-03-03" "2022-04-08" "2022-05-04")
            (are [pvm] (not (nh/jakso-active? jakso-2 opiskeluoikeus-2 (LocalDate/parse pvm)))
              "2022-02-02" "2022-03-21" "2023-01-01"))
          (testing "ei ole jakson alku- ja loppupäivämäärien välillä."
            (are [pvm] (not (nh/jakso-active? jakso-1 opiskeluoikeus-1 (LocalDate/parse pvm)))
              "2022-01-02" "2022-05-06" "2021-12-30" "2023-01-01")
            (are [pvm] (not (nh/jakso-active? jakso-2 opiskeluoikeus-1 (LocalDate/parse pvm)))
              "2021-12-31" "2021-03-21" "2019-01-01")))))))

(deftest test-get-keskeytymisajanjaksot
  (testing "Funktio palauttaa jakson tai tähän liitetyn opiskeluoikeuden
           sisältämät keskeytymisajanjaksot"
    (are [jakso opiskeluoikeus expected] (= (nh/get-keskeytymisajanjaksot jakso opiskeluoikeus)
                                            (map c/alku-and-loppu-to-localdate expected))
      {} {} '()
      ; ---
      {:keskeytymisajanjaksot [{:alku "2022-01-12" :loppu "2022-01-12"}
                               {:alku "2022-03-03" :loppu "2022-04-30"}]}
      {}
      [{:alku "2022-01-12" :loppu "2022-01-12"}
       {:alku "2022-03-03" :loppu "2022-04-30"}]
      ; ---
      {} (mock-get-opiskeluoikeus-catch-404 "1.2.3.4")
      []
      ; ---
      {} (mock-get-opiskeluoikeus-catch-404 "1.2.3.5")
      [{:tila {:koodiarvo "valiaikaisestikeskeytynyt"} :alku "2022-02-02"}]
      ; ---
      (nth jaksot-1 2) (mock-get-opiskeluoikeus-catch-404 "1.2.3.5")
      [{:alku "2022-02-16" :loppu "2022-02-18"}
       {:alku "2022-02-02" :tila {:koodiarvo "valiaikaisestikeskeytynyt"}}])))

(deftest test-calculate-single-day-kestot
  (testing "Funktio palauttaa yhden päivän aktiivisten jaksojen kestot"
    (let [opiskeluoikeudet (reduce
                             #(assoc %
                                     %2
                                     (mock-get-opiskeluoikeus-catch-404 %2))
                             {}
                             (map :opiskeluoikeus_oid jaksot-2))
          jaksot (map (comp c/alku-and-loppu-to-localdate nh/harmonize-date-keys) jaksot-2)]
      (are [pvm kesto jakso-ids] (= (do-rounding
                                      (nh/calculate-single-day-kestot
                                        jaksot
                                        opiskeluoikeudet
                                        (LocalDate/parse pvm)))
                                    (zipmap jakso-ids (repeat kesto)))
           "2021-01-12" 0     []
           "2021-01-27" 1.0   [7]
           "2021-02-03" 0.5   [7 16]
           "2021-05-15" 0.333 [4 7 16]
           "2021-06-15" 0.5   [4 7]
           "2021-08-19" 0.333 [4 7 9]
           "2021-08-24" 0.25  [4 7 9 10]
           "2021-08-28" 0.333 [4 7 10]
           "2021-09-02" 0.167 [2 4 5 7 10 14]
           "2021-09-03" 0.143 [2 4 5 7 10 11 14]
           "2021-09-06" 0.125 [2 4 5 7 10 11 13 14]
           "2021-09-18" 0.143 [2 4 5 7 11 13 14]
           "2021-10-01" 0.167 [2 4 5 7 13 14]
           "2021-10-15" 0.125 [2 3 4 5 7 8 14 15]
           "2021-10-31" 0.167 [2 4 5 7 8 14]
           "2021-11-14" 0.143 [2 4 5 7 8 12 14]
           "2021-11-22" 0.167 [2 4 5 7 8 14]
           "2021-12-05" 0.167 [2 5 7 8 12 14]
           "2021-12-17" 0.333 [5 7 14]
           "2022-06-04" 0.5   [5 14]
           "2024-07-21" 1.0   [14]
           "2024-12-16" 0     []))))

(deftest test-calculate-kestot-old
  (testing "`calculate-kestot-old` laskee kestot oikein."
    (is (= (nh/calculate-kestot-old jaksot-1
                                    (get-test-opiskeluoikeudet
                                      (map :opiskeluoikeus_oid jaksot-1)))
           {1 11
            2 18
            3 5
            4 6
            5 9}))))

(deftest test-calculate-kestot
  (testing "`calculate-kestot` laskee kestot oikein."
      (is (= (nh/calculate-kestot
                            jaksot-2
                            (get-test-opiskeluoikeudet
                              (map :opiskeluoikeus_oid jaksot-2)))
             {1 1
              2 15
              3 1
              4 53
              5 359
              6 0
              7 169
              8 9
              9 18
              10 5
              11 4
              12 4
              13 5
              14 725
              15 2
              16 62
              17 0}))))

(deftest test-calculate-kestot!
  (testing "`calculate-kestot!` laskee oikein työpaikkajakson kestot sekä
            uudella ja vanhalla laskentatavalla."
    (with-redefs
     [oph.heratepalvelu.external.ehoks/get-tyoelamajaksot-active-between!
      (fn [oppija-oid start end]
        (reset! get-tyoelamajaksot-active-between-call-params
                {:start start :end end :oppija oppija-oid})
        jaksot-1)
      koski/get-opiskeluoikeus-catch-404! mock-get-opiskeluoikeus-catch-404]
      (is (= (nh/calculate-kestot! [{:oppija_oid "4.4.4.4"
                                     :jakso_alkupvm "2022-01-05"
                                     :jakso_loppupvm "2022-02-28"}
                                    {:oppija_oid "4.4.4.4"
                                     :jakso_alkupvm "2022-01-10"
                                     :jakso_loppupvm "2022-03-03"}])
             [{1 14  2 50  3 0  4 15  5 14}      ; uudella laskentatavalla
              {1 11  2 18  3 5  4 6   5 9}]))))) ; vanhalla laskentatavalla

(deftest test-query-jaksot
  (testing "Varmistaa, että query-jaksot toimii oikein."
    (with-redefs [environ.core/env {:jaksotunnus-table "table-name"}
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 3 3))
                  oph.heratepalvelu.db.dynamodb/query-items
                  (fn [query-params options table]
                    {:query-params query-params :options options :table table})]
      (let [nippu {:ohjaaja_ytunnus_kj_tutkinto "asdf"
                   :niputuspvm "2022-02-02"}
            results {:query-params
                     {:ohjaaja_ytunnus_kj_tutkinto [:eq [:s "asdf"]]
                      :niputuspvm [:eq [:s "2022-02-02"]]}
                     :options
                     {:index "niputusIndex"
                      :filter-expression
                      "#pvm >= :pvm AND attribute_exists(#tunnus)"
                      :expr-attr-names {"#pvm"    "viimeinen_vastauspvm"
                                        "#tunnus" "tunnus"}
                      :expr-attr-vals {":pvm" [:s "2022-03-03"]}}
                     :table "table-name"}]
        (is (= (nh/query-jaksot! nippu) results))))))

(def trauj-results (atom []))

(defn- add-to-trauj-results [item]
  (reset! trauj-results (conj @trauj-results item)))

(defn- mock-trauj-query-jaksot [nippu]
  (add-to-trauj-results {:type "query-jaksot" :nippu nippu})
  [{:hankkimistapa_id 1} {:hankkimistapa_id 2} {:hankkimistapa_id 3}])

(defn- mock-trauj-group-jaksot-and-calculate-kestot [jaksot]
  (add-to-trauj-results {:type "group-jaksot-and-calculate-kestot"
                         :jaksot jaksot})
  [{1 1 2 3 3 5} {1 1 2 2 3 4}])

(defn- mock-trauj-update-jakso [jakso updates]
  (add-to-trauj-results {:type "update-jakso" :jakso jakso :updates updates}))

(deftest test-retrieve-and-update-jaksot
  (testing "Varmistaa, että retrieve-and-update-jaksot toimii oikein."
    (with-redefs
      [nh/calculate-kestot!
       mock-trauj-group-jaksot-and-calculate-kestot
       nh/query-jaksot! mock-trauj-query-jaksot
       oph.heratepalvelu.tep.tepCommon/update-jakso mock-trauj-update-jakso]
      (let [nippu {:mock-nippu-contents "asdf"}
            results [{:type "query-jaksot" :nippu {:mock-nippu-contents "asdf"}}
                     {:type "group-jaksot-and-calculate-kestot"
                      :jaksot [{:hankkimistapa_id 1}
                               {:hankkimistapa_id 2}
                               {:hankkimistapa_id 3}]}
                     {:type "update-jakso"
                      :jakso {:hankkimistapa_id 1}
                      :updates {:kesto [:n 1] :kesto-vanha [:n 1]}}
                     {:type "update-jakso"
                      :jakso {:hankkimistapa_id 2}
                      :updates {:kesto [:n 3] :kesto-vanha [:n 2]}}
                     {:type "update-jakso"
                      :jakso {:hankkimistapa_id 3}
                      :updates {:kesto [:n 5] :kesto-vanha [:n 4]}}]
            updated-jaksot [{:hankkimistapa_id 1 :kesto 1}
                            {:hankkimistapa_id 2 :kesto 3}
                            {:hankkimistapa_id 3 :kesto 5}]]
        (is (= (nh/retrieve-and-update-jaksot! nippu) updated-jaksot))
        (is (= @trauj-results results))))))

(def test-niputa-results (atom []))

(defn- add-to-test-niputa-results [data]
  (reset! test-niputa-results (cons data @test-niputa-results)))

(defn- mock-generate-uuid [] "test-uuid")

(defn- mock-create-nippu-kyselylinkki [niputus-request-body]
  (add-to-test-niputa-results {:type "mock-create-nippu-kyselylinkki"
                               :niputus-request-body niputus-request-body})
  {:nippulinkki (when (= "123456-7" (:tyonantaja niputus-request-body))
                  "kysely.linkki/132")
   :voimassa_loppupvm "2021-12-17"})

(defn- mock-delete-nippukyselylinkki [tunniste]
  (add-to-test-niputa-results {:type "mock-delete-nippukyselylinkki"
                               :tunniste tunniste}))

(defn- mock-update-nippu
  ([nippu updates] (mock-update-nippu nippu updates {}))
  ([nippu updates options]
   (add-to-test-niputa-results {:type "mock-update-nippu"
                                :nippu nippu
                                :updates updates
                                :options options})))

(defn- mock-retrieve-and-update-jaksot [nippu]
  (add-to-test-niputa-results {:type "mock-retrieve-and-update-jaksot"
                               :nippu nippu})
  (if (not= (:ohjaaja_ytunnus_kj_tutkinto nippu) "test-id-0")
    [{:tunnus "ABCDEF"
      :kesto 1.5
      :tyopaikan_nimi "Testi Työ Paikka"
      :viimeinen_vastauspvm "2022-02-02"}]
    []))

(deftest test-niputa
  (testing "Varmista, että niputa-funktio tekee oikeita kutsuja"
    (with-redefs
      [environ.core/env {:jaksotunnus-table "jaksotunnus-table-name"
                         :nippu-table "nippu-table-name"}
       oph.heratepalvelu.common/generate-uuid mock-generate-uuid
       oph.heratepalvelu.common/local-date-now (fn [] (LocalDate/of 2021 12 31))
       oph.heratepalvelu.common/rand-str (fn [_] "abcdef")
       arvo/create-nippu-kyselylinkki mock-create-nippu-kyselylinkki
       arvo/delete-nippukyselylinkki mock-delete-nippukyselylinkki
       oph.heratepalvelu.tep.niputusHandler/retrieve-and-update-jaksot!
       mock-retrieve-and-update-jaksot
       oph.heratepalvelu.tep.tepCommon/update-nippu mock-update-nippu]
      (let [test-nippu-0 {:ohjaaja_ytunnus_kj_tutkinto "test-id-0"
                          :niputuspvm "2021-12-15"}
            test-nippu-1 {:ohjaaja_ytunnus_kj_tutkinto "test-id-1"
                          :niputuspvm "2021-12-15"
                          :tyopaikka "Testityöpaikka"
                          :ytunnus "123456-7"
                          :koulutuksenjarjestaja "12345"
                          :tutkinto "asdf"}
            test-nippu-2 {:ohjaaja_ytunnus_kj_tutkinto "test-id-2"
                          :niputuspvm "2021-12-15"
                          :ytunnus "111111-1"
                          :koulutuksenjarjestaja "12111"
                          :tutkinto "aaaa"}
            results [{:type "mock-retrieve-and-update-jaksot"
                      :nippu test-nippu-0}
                     {:type "mock-update-nippu"
                      :nippu {:ohjaaja_ytunnus_kj_tutkinto "test-id-0"
                              :niputuspvm "2021-12-15"}
                      :updates
                      {:kasittelytila [:s (:ei-jaksoja c/kasittelytilat)]
                       :request_id [:s "test-uuid"]
                       :kasittelypvm [:s "2021-12-31"]}
                      :options {}}
                     {:type "mock-retrieve-and-update-jaksot"
                      :nippu test-nippu-1}
                     {:type "mock-create-nippu-kyselylinkki"
                      :niputus-request-body
                      {:tunniste "testi_tyo_paikka_2021-12-31_abcdef"
                       :koulutustoimija_oid "12345"
                       :tutkintotunnus "asdf"
                       :tyonantaja "123456-7"
                       :tyopaikka "Testityöpaikka"
                       :tunnukset [{:tunnus "ABCDEF"
                                    :tyopaikkajakson_kesto 1.5}]
                       :voimassa_alkupvm "2021-12-31"
                       :request_id "test-uuid"}}
                     {:type "mock-update-nippu"
                      :nippu {:ohjaaja_ytunnus_kj_tutkinto "test-id-1"
                              :niputuspvm "2021-12-15"
                              :tyopaikka "Testityöpaikka"
                              :ytunnus "123456-7"
                              :koulutuksenjarjestaja "12345"
                              :tutkinto "asdf"}
                      :updates
                      {:kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]
                       :kyselylinkki [:s "kysely.linkki/132"]
                       :voimassaloppupvm [:s "2021-12-17"]
                       :request_id [:s "test-uuid"]
                       :kasittelypvm [:s "2021-12-31"]}
                      :options
                      {:cond-expr "attribute_not_exists(kyselylinkki)"}}
                     {:type "mock-retrieve-and-update-jaksot"
                      :nippu test-nippu-2}
                     {:type "mock-create-nippu-kyselylinkki"
                      :niputus-request-body
                      {:tunniste "testi_tyo_paikka_2021-12-31_abcdef"
                       :koulutustoimija_oid "12111"
                       :tutkintotunnus "aaaa"
                       :tyonantaja "111111-1"
                       :tyopaikka nil
                       :tunnukset [{:tunnus "ABCDEF"
                                    :tyopaikkajakson_kesto 1.5}]
                       :voimassa_alkupvm "2021-12-31"
                       :request_id "test-uuid"}}
                     {:type "mock-update-nippu"
                      :nippu {:ohjaaja_ytunnus_kj_tutkinto "test-id-2"
                              :niputuspvm "2021-12-15"
                              :ytunnus "111111-1"
                              :koulutuksenjarjestaja "12111"
                              :tutkinto "aaaa"}
                      :updates
                      {:kasittelytila [:s (:niputusvirhe c/kasittelytilat)]
                       :kasittelypvm [:s "2021-12-31"]
                       :reason [:s "no reason in response"]
                       :request_id [:s "test-uuid"]}
                      :options {}}]]
        (nh/niputa test-nippu-0)
        (nh/niputa test-nippu-1)
        (nh/niputa test-nippu-2)
        (is (= results (vec (reverse @test-niputa-results))))))))

(def handler-results (atom []))

(defn- add-to-handler-results [data]
  (reset! handler-results (cons data @handler-results)))

(defn- mock-handler-query-items [query-params options table]
  (when (and (= :eq (first (:kasittelytila query-params)))
             (= :s (first (second (:kasittelytila query-params))))
             (= (:ei-niputettu c/kasittelytilat)
                (second (second (:kasittelytila query-params))))
             (= :le (first (:niputuspvm query-params)))
             (= :s (first (second (:niputuspvm query-params))))
             (= "niputusIndex" (:index options))
             (= 10 (:limit options))
             (= "nippu-table-name" table))
    (add-to-handler-results
      {:type "mock-handler-query-items"
       :date (second (second (:niputuspvm query-params)))})
    [{:id 2 :niputuspvm "2021-11-30"}
     {:id 1 :niputuspvm "2021-11-11"}
     {:id 3 :niputuspvm "2021-12-10"}]))

(defn- mock-niputa [nippu]
  (add-to-handler-results {:type "mock-niputa" :nippu nippu}))

(deftest test-handleNiputus
  (testing "Varmista, että -handleNiputus tekee oikeita kutsuja"
    (with-redefs
      [environ.core/env {:nippu-table "nippu-table-name"}
       oph.heratepalvelu.common/local-date-now (fn [] (LocalDate/of 2021 12 15))
       oph.heratepalvelu.db.dynamodb/query-items mock-handler-query-items
       oph.heratepalvelu.tep.niputusHandler/niputa mock-niputa]
      (let [event (tu/mock-handler-event :scheduledherate)
            context (tu/mock-handler-context)
            results [{:type "mock-handler-query-items"
                      :date "2021-12-15"}
                     {:type "mock-niputa"
                      :nippu {:id 3
                              :niputuspvm "2021-12-10"}}
                     {:type "mock-niputa"
                      :nippu {:id 2
                              :niputuspvm "2021-11-30"}}
                     {:type "mock-niputa"
                      :nippu {:id 1
                              :niputuspvm "2021-11-11"}}]]
        (nh/-handleNiputus {} event context)
        (is (= results (vec (reverse @handler-results))))))))
