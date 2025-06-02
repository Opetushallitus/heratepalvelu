(ns oph.heratepalvelu.tep.niputusHandler-test
  (:require [clojure.test :refer [are deftest is testing]]
            [medley.core :refer [map-keys filter-vals]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.tep.niputusHandler :as nh]
            [oph.heratepalvelu.external.ehoks :as ehoks]
            [oph.heratepalvelu.external.koski :as koski]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(def jakso-1 {:opiskeluoikeus_oid "1.2.3.8"
              :oppija_oid "4.4.4.4"
              :hoks_id 1
              :yksiloiva_tunniste "1"
              :hankkimistapa_id 1
              :jakso_alkupvm "2021-10-22"
              :jakso_loppupvm "2021-10-25"
              :osa_aikaisuus 40
              :keskeytymisajanjaksot [{:id 1032
                                       :osaamisen-hankkimistapa-id 1
                                       :alku "2022-02-11"
                                       :loppu "3022-03-01"}]})

(def jakso-2 {:opiskeluoikeus_oid "1.2.3.8"
              :oppija_oid "4.4.4.4"
              :hoks_id 1
              :yksiloiva_tunniste "2"
              :hankkimistapa_id 2
              :jakso_alkupvm "2021-09-01"
              :jakso_loppupvm "2021-12-15"
              :osa_aikaisuus 80
              :keskeytymisajanjaksot [{:id 1033
                                       :osaamisen-hankkimistapa-id 2
                                       :alku "2021-10-22"
                                       :loppu "2021-10-30"}]})

(def jakso-3 {:opiskeluoikeus_oid "1.2.3.8"
              :oppija_oid "4.4.4.4"
              :hoks_id 1
              :yksiloiva_tunniste "3"
              :hankkimistapa_id 3
              :jakso_alkupvm "2021-10-11"
              :jakso_loppupvm "2021-10-21"
              :osa_aikaisuus 100
              :keskeytymisajanjaksot [{:id 1030
                                       :osaamisen-hankkimistapa-id 3
                                       :alku "2021-10-13"
                                       :loppu "2021-10-14"}]})

(def jakso-4 {:opiskeluoikeus_oid "1.2.3.8"
              :oppija_oid "4.4.4.4"
              :hoks_id 1
              :yksiloiva_tunniste "4"
              :hankkimistapa_id 4
              :jakso_alkupvm "2021-05-15"
              :jakso_loppupvm "2021-12-01"
              :osa_aikaisuus 70
              :keskeytymisajanjaksot []})

(def jakso-5 {:opiskeluoikeus_oid "1.2.3.7"
              :oppija_oid "4.4.4.4"
              :hoks_id 2
              :yksiloiva_tunniste "1"
              :hankkimistapa_id 5
              :jakso_alkupvm "2021-09-01"
              :jakso_loppupvm "2023-12-15"
              :osa_aikaisuus 60
              :keskeytymisajanjaksot []})

(def jakso-6 {:opiskeluoikeus_oid "1.2.3.8"
              :oppija_oid "4.4.4.4"
              :hoks_id 1
              :yksiloiva_tunniste "5"
              :hankkimistapa_id 6
              :jakso_alkupvm "2021-10-11"
              :jakso_loppupvm "2021-10-21"
              :osa_aikaisuus 100
              :keskeytymisajanjaksot [{:id 1031
                                       :osaamisen-hankkimistapa-id 6
                                       :alku "2021-10-13"
                                       :loppu "2021-10-14"}]})

(def jakso-7 {:opiskeluoikeus_oid "1.2.3.10"
              :oppija_oid "4.4.4.4"
              :hoks_id 3
              :yksiloiva_tunniste "1"
              :hankkimistapa_id 7
              :jakso_alkupvm "2021-01-15"
              :jakso_loppupvm "2022-04-29"
              :osa_aikaisuus 50
              :keskeytymisajanjaksot []})

(def jakso-8 {:opiskeluoikeus_oid "1.2.3.8"
              :oppija_oid "4.4.4.4"
              :hoks_id 1
              :yksiloiva_tunniste "6"
              :hankkimistapa_id 8
              :jakso_alkupvm "2021-10-15"
              :jakso_loppupvm "2021-12-11"
              :osa_aikaisuus 100
              :keskeytymisajanjaksot []})

(def jakso-9 {:opiskeluoikeus_oid "1.2.3.6"
              :oppija_oid "4.4.4.4"
              :hoks_id 4
              :yksiloiva_tunniste "1"
              :hankkimistapa_id 9
              :jakso_alkupvm "2021-07-03"
              :jakso_loppupvm "2021-08-26"
              :osa_aikaisuus 100
              :keskeytymisajanjaksot []})

(def jakso-10 {:opiskeluoikeus_oid "1.2.3.7"
               :oppija_oid "4.4.4.4"
               :hoks_id 2
               :yksiloiva_tunniste "2"
               :hankkimistapa_id 10
               :jakso_alkupvm "2021-08-20"
               :jakso_loppupvm "2021-09-10"
               :osa_aikaisuus 100
               :keskeytymisajanjaksot [{:id 1191
                                        :osaamisen-hankkimistapa-id 10
                                        :alku  "2021-08-25"
                                        :loppu "2021-08-28"}]})

(def jakso-11 {:opiskeluoikeus_oid "1.2.3.7"
               :oppija_oid "4.4.4.4"
               :hoks_id 2
               :yksiloiva_tunniste "3"
               :hankkimistapa_id 11
               :jakso_alkupvm "2021-09-03"
               :jakso_loppupvm "2021-09-30"
               :osa_aikaisuus 100
               :keskeytymisajanjaksot []})

(def jakso-12 {:opiskeluoikeus_oid "1.2.3.7"
               :oppija_oid "4.4.4.4"
               :hoks_id 2
               :yksiloiva_tunniste "4"
               :hankkimistapa_id 12
               :jakso_alkupvm "2021-11-12"
               :jakso_loppupvm "2021-12-15"
               :osa_aikaisuus 100
               :keskeytymisajanjaksot [{:id 1192
                                        :osaamisen-hankkimistapa-id 12
                                        :alku "2021-11-18"
                                        :loppu "2021-11-25"}]})

(def jakso-13 {:opiskeluoikeus_oid "1.2.3.8"
               :oppija_oid "4.4.4.4"
               :hoks_id 1
               :yksiloiva_tunniste "7"
               :hankkimistapa_id 13
               :jakso_alkupvm "2021-09-06"
               :jakso_loppupvm "2021-10-19"
               :osa_aikaisuus 100
               :keskeytymisajanjaksot [{:id 1029
                                        :osaamisen-hankkimistapa-id 13
                                        :alku "2021-10-13"}]})

(def jakso-14 {:opiskeluoikeus_oid "1.2.3.7"
               :oppija_oid "4.4.4.4"
               :hoks_id 2
               :yksiloiva_tunniste "5"
               :hankkimistapa_id 14
               :jakso_alkupvm "2021-09-01"
               :jakso_loppupvm "2024-12-15"
               :osa_aikaisuus 100
               :keskeytymisajanjaksot []})

(def jakso-15 {:opiskeluoikeus_oid "1.2.3.8"
               :oppija_oid "4.4.4.4"
               :hoks_id 1
               :yksiloiva_tunniste "8"
               :hankkimistapa_id 15
               :jakso_alkupvm "2021-10-04"
               :jakso_loppupvm "2021-10-19"
               :osa_aikaisuus 100
               :keskeytymisajanjaksot []})

(def jakso-16 {:opiskeluoikeus_oid "1.2.3.9"
               :oppija_oid "4.4.4.4"
               :hoks_id 5
               :yksiloiva_tunniste "1"
               :hankkimistapa_id 16
               :jakso_alkupvm "2021-02-01"
               :jakso_loppupvm "2021-08-30"
               :osa_aikaisuus 100
               :keskeytymisajanjaksot [{:id 1034
                                        :osaamisen-hankkimistapa-id 16
                                        :alku "2021-06-15"}]})

(def jakso-17 {:opiskeluoikeus_oid "1.2.3.10.onnea.etsintaan"
               :oppija_oid "4.4.4.4"
               :hoks_id 6
               :yksiloiva_tunniste "1"
               :hankkimistapa_id 17
               :jakso_alkupvm "2021-08-27"
               :jakso_loppupvm "2022-04-29"
               :osa_aikaisuus 100
               :keskeytymisajanjaksot []})

(def jakso-21 {:hankkimistapa_id 21
               :hoks_id 7
               :yksiloiva_tunniste "1"
               :oppija_oid "3.3.3.3"
               :osa_aikaisuus 100
               :jakso_alkupvm "2022-01-03"
               :jakso_loppupvm "2022-02-05"
               :keskeytymisajanjaksot []
               :opiskeluoikeus_oid "1.2.3.1"})

(def jakso-22 {:hankkimistapa_id 22
               :hoks_id 8
               :yksiloiva_tunniste "1"
               :oppija_oid "3.3.3.3"
               :osa_aikaisuus 50
               :jakso_alkupvm "2022-02-07"
               :jakso_loppupvm "2022-04-04"
               :opiskeluoikeus_oid "1.2.3.2"})

(def jakso-23 {:hankkimistapa_id 23
               :hoks_id 9
               :yksiloiva_tunniste "1"
               :oppija_oid "3.3.3.3"
               :osa_aikaisuus 20
               :jakso_alkupvm "2022-01-31"
               :jakso_loppupvm "2022-02-20"
               :keskeytymisajanjaksot [{:alku "2022-02-16"
                                        :loppu "2022-02-18"}]
               :opiskeluoikeus_oid "1.2.3.3"})

(def jakso-24 {:hankkimistapa_id 24
               :hoks_id 10
               :yksiloiva_tunniste "1"
               :oppija_oid "3.3.3.3"
               :osa_aikaisuus 90
               :jakso_alkupvm "2022-01-17"
               :jakso_loppupvm "2022-02-20"
               :opiskeluoikeus_oid "1.2.3.4"})

(def jakso-25 {:hankkimistapa_id 25
               :hoks_id 11
               :yksiloiva_tunniste "1"
               :oppija_oid "3.3.3.3"
               :osa_aikaisuus 100
               :jakso_alkupvm "2022-01-01"
               :jakso_loppupvm "2022-04-01"
               :keskeytymisajanjaksot [{:alku "2022-01-12" :loppu "2022-01-12"}
                                       {:alku "2022-01-16" :loppu "2022-01-18"}]
               :opiskeluoikeus_oid "1.2.3.5"})

(def jaksot-1-17
  [jakso-1  jakso-2  jakso-3 jakso-4 jakso-5 jakso-6 jakso-7 jakso-8 jakso-9
   jakso-10 jakso-11 jakso-12 jakso-13 jakso-14 jakso-15 jakso-16 jakso-17])

(def jaksot-21-25 [jakso-21 jakso-22 jakso-23 jakso-24 jakso-25])

(def jaksot-1-25 (concat jaksot-1-17 jaksot-21-25))

(def opiskeluoikeudet
  {"1.2.3.1" {:tila {:opiskeluoikeusjaksot
                     [{:alku "2022-01-01" :tila {:koodiarvo "lasna"}}]}}
   "1.2.3.2" {:tila {:opiskeluoikeusjaksot
                     [{:alku "2022-01-01" :tila {:koodiarvo "lasna"}}]}}
   "1.2.3.4" {:tila {:opiskeluoikeusjaksot
                     [{:alku "2022-01-01" :tila {:koodiarvo "lasna"}}
                      {:alku "2022-01-30" :tila {:koodiarvo "loma"}}
                      {:alku "2022-02-10" :tila {:koodiarvo "lasna"}}]}}
   "1.2.3.5" {:tila {:opiskeluoikeusjaksot
                     [{:alku "2022-01-01" :tila {:koodiarvo "lasna"}}
                      {:alku "2022-02-02"
                       :tila {:koodiarvo "valiaikaisestikeskeytynyt"}}]}}
   "1.2.3.6" {:tila {:opiskeluoikeusjaksot
                     [{:alku "2020-03-05", :tila {:koodiarvo "lasna"}}]}}
   "1.2.3.7" {:tila {:opiskeluoikeusjaksot
                     [{:alku "2020-04-06", :tila {:koodiarvo "lasna"}}]}}
   "1.2.3.8" {:tila {:opiskeluoikeusjaksot
                     [{:alku "2020-01-01", :tila {:koodiarvo "lasna"}}]}}
   "1.2.3.9" {:tila {:opiskeluoikeusjaksot
                     [{:alku "2022-08-25", :tila {:koodiarvo "lasna"}}]}}
   "1.2.3.10" {:tila {:opiskeluoikeusjaksot
                      [{:alku "2020-10-27", :tila {:koodiarvo "lasna"}}]}}})

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

(def mock-get-opiskeluoikeus-catch-404-count (atom 0))

(defn- do-rounding [values]
  (reduce-kv #(assoc %1 %2 (/ (Math/round (* %3 100.0)) 100.0)) {} values))

(defn- mock-get-opiskeluoikeus-catch-404 [oo-oid]
  (assert oo-oid "Opiskeluoikeus OID `oo-oid` puuttuu")
  (swap! mock-get-opiskeluoikeus-catch-404-count inc)
  (get opiskeluoikeudet oo-oid))

(deftest test-get-and-memoize-opiskeluoikeudet
  (with-redefs
    [clojure.tools.logging/log* tu/mock-log*
     koski/get-opiskeluoikeus-catch-404! mock-get-opiskeluoikeus-catch-404]
    (testing
      (str "Funktio pitää entuudestaan haetut opiskeluoikeudet muistitssa, "
           "eikä hae niitä toistamiseen. Funktio lokitaa varoituksen, jos "
           "opiskeluoikeutta ei saada koskesta")
      (is (= (nh/get-and-memoize-opiskeluoikeudet! []) {}))
      (reset! mock-get-opiskeluoikeus-catch-404-count 0)
      (is (= (nh/get-and-memoize-opiskeluoikeudet!
               [jakso-1   ; 1.2.3.8
                jakso-2   ; 1.2.3.8
                jakso-5   ; 1.2.3.7
                jakso-6   ; 1.2.3.8
                jakso-10  ; 1.2.3.7
                {:hoks_id 1
                 :yksiloiva_tunniste "123"
                 :hankkimistapa_id 123
                 :opiskeluoikeus_oid "1.2.3.4.ei.loydy"}])
             {"1.2.3.7" (get opiskeluoikeudet "1.2.3.7")
              "1.2.3.8" (get opiskeluoikeudet "1.2.3.8")}))
      (is (= @mock-get-opiskeluoikeus-catch-404-count 3))
      (is (tu/logs-contain?
            {:level   :warn
             :message (str "Opiskeluoikeutta `1.2.3.4.ei.loydy` ei saatu "
                           "Koskesta. Jakson (HOKS `1`, yksilöivä tunniste "
                           "`123`) kestoksi asetetaan nolla.")})))))

(deftest test-get-opiskeluoikeusjaksot
  (testing "Funktio hakee onnistuneesti opiskeluoikeuden opiskeluoikeusjaksot."
    (are [oo-oid expected] (= (nh/get-opiskeluoikeusjaksot
                                (mock-get-opiskeluoikeus-catch-404 oo-oid))
                              expected)
      "1.2.3.4" (map c/alku-and-loppu-to-localdate
                     [{:tila {:koodiarvo "lasna"}
                       :alku "2022-01-01"
                       :loppu "2022-01-29"}
                      {:tila {:koodiarvo "loma"}
                       :alku "2022-01-30"
                       :loppu "2022-02-09"}
                      {:tila {:koodiarvo "lasna"} :alku "2022-02-10"}])
      "1.2.3.5" (map c/alku-and-loppu-to-localdate
                     [{:tila {:koodiarvo "lasna"}
                       :alku "2022-01-01"
                       :loppu "2022-02-01"}
                      {:tila {:koodiarvo "valiaikaisestikeskeytynyt"}
                       :alku "2022-02-02"}]))))

(defn- mock-get-tyoelamajaksot-active-between!
  [oppija-oid start end]
  (assert oppija-oid "oppija-oid puuttuu")
  (assert start "aikavälin alkupäivämäärä `start` puuttuu")
  (assert end "aikavälin päättymispäivämäärä `end` puuttuu")
  (filter #(and (= (:oppija_oid %) oppija-oid)
                (<= (compare (:jakso_alkupvm %) end) 0)
                (>= (compare (:jakso_loppupvm %) start) 0))
          jaksot-1-25))

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
                 :keskeytymisajanjaksot [{:alku "2022-02-20"
                                          :loppu "2022-03-03"}
                                         {:alku "2022-04-08"}]}
        jakso-2 {:alku (LocalDate/parse "2022-01-01")}
        opiskeluoikeus-1 (mock-get-opiskeluoikeus-catch-404 "1.2.3.4")
        opiskeluoikeus-2 (mock-get-opiskeluoikeus-catch-404 "1.2.3.5")]
    (testing "Funktio palauttaa"
      (testing (str "`true`, kun päivämäärä on jakson sisällä, eikä se kuulu "
                    "mihinkään keskeytymisajanjaksoon.")
        (are [pvm] (nh/jakso-active? jakso-1 opiskeluoikeus-1
                                     (LocalDate/parse pvm))
          "2022-01-03" "2022-02-19" "2022-03-04" "2022-04-07" "2022-02-10")
        (are [pvm] (nh/jakso-active? jakso-2
                                     opiskeluoikeus-2
                                     (LocalDate/parse pvm))
          "2022-01-01" "2022-01-21" "2022-02-01"))
      (testing "`false`,"
        (testing "kun opiskeluoikeus on `nil`."
          (are [pvm] (not (nh/jakso-active? jakso-1 nil (LocalDate/parse pvm)))
            "2022-01-03" "2022-02-19" "2022-03-04" "2022-04-07" "2022-02-10"))
        (testing "kun päivämäärä"
          (testing "sisältyy keskeytymisajanjaksoon."
            (are [pvm] (not (nh/jakso-active? jakso-1
                                              opiskeluoikeus-1
                                              (LocalDate/parse pvm)))
              "2022-02-20" "2022-03-01" "2022-03-03" "2022-04-08" "2022-05-04")
            (are [pvm] (not (nh/jakso-active? jakso-2
                                              opiskeluoikeus-2
                                              (LocalDate/parse pvm)))
              "2022-02-02" "2022-03-21" "2023-01-01"))
          (testing "ei ole jakson alku- ja loppupäivämäärien välillä."
            (are [pvm] (not (nh/jakso-active? jakso-1
                                              opiskeluoikeus-1
                                              (LocalDate/parse pvm)))
              "2022-01-02" "2022-05-06" "2021-12-30" "2023-01-01")
            (are [pvm] (not (nh/jakso-active? jakso-2
                                              opiskeluoikeus-1
                                              (LocalDate/parse pvm)))
              "2021-12-31" "2021-03-21" "2019-01-01")))))))

(deftest test-osa-aikaisuuskerroin
  (with-redefs [clojure.tools.logging/log* tu/mock-log*]
    (testing (str "Jos osa-aikaisuustieto puuttuu jakson tiedosta, tulkitaan "
                  "jakson osa-aikaisuudeksi 0 %.")
      (is (= (nh/osa-aikaisuuskerroin {:hoks_id 1
                                       :yksiloiva_tunniste "123"
                                       :hankkimistapa_id 123})
             0))
      (is (tu/logs-contain?
            {:level   :warn
             :message (str "Osa-aikaisuustieto puuttuu jakson (HOKS `1`, "
                           "yksilöivä tunniste `123`) tiedoista. "
                           "Jakson kestoksi asetetaan nolla.")})))
    (testing (str "Osa-aikaisuustiedon ollessa ei-validi, osa-aikaisuuden "
                  "katsotaan olevan 0 %.")
      (are [oa] (true? (and (= (nh/osa-aikaisuuskerroin {:hoks_id 1
                                                         :yksiloiva_tunniste 123
                                                         :hankkimistapa_id 123
                                                         :osa_aikaisuus oa})
                               0)
                            (tu/logs-contain?
                              {:level :warn
                               :message (str "Jakson (HOKS `1`, yksilöivä "
                                             "tunniste `123`) osa-aikaisuus `"
                                             oa
                                             "` ei ole validi. Jakson kestoksi "
                                             "asetetaan nolla.")})))
        -1 0 120 5.5 "100")))
  (testing (str "Osa-aikaisuustiedon ollessa validi, funktio hakee "
                "osa-aikaisuustiedon jaksosta ja palauttaa tämän "
                "perusteella lasketun osa-aikaisuuskertoimen.")
    (are [oa] (= (nh/osa-aikaisuuskerroin {:osa_aikaisuus oa})
                 (/ oa 100.0))
      1 15 50 98 100)))

(deftest test-keskeytymisajanjaksot
  (testing
    (str "Funktio ei palauta yhtään keskeytymisajanjaksoa, kun parametreiksi "
         "ei ole annettu yhtään jaksoa eikä opiskeluoikeutta.")
    (is (= (nh/keskeytymisajanjaksot {} {}) '())))
  (testing
    (str "Funktio ei palauta yhtään keskeytymisajanjaksoa, kun jakson eikä "
         "opiskeluoikeuden tietoihin ole merkitty keskeytymisajanjaksoja.")
    (is (= (nh/keskeytymisajanjaksot
             jakso-4 {mock-get-opiskeluoikeus-catch-404 "1.2.3.8"})
           '())))
  (let [jakso-kjaksot  [{:alku "2022-01-12" :loppu "2022-01-12"}
                        {:alku "2022-01-16" :loppu "2022-01-18"}]
        opiskeluoikeus (mock-get-opiskeluoikeus-catch-404 "1.2.3.5")
        oo-kjaksot     (map c/alku-and-loppu-to-localdate
                            [{:alku "2022-02-02"
                              :tila {:koodiarvo "valiaikaisestikeskeytynyt"}}])]
    (testing
      (str "Keskeytymisajanjakso jaksossa, muttei opiskeluoikeudessa. Funktio "
           "palauttaa pelkästään jakson keskeytymisajanjaksot.")
      (is (= (nh/keskeytymisajanjaksot
               {:keskeytymisajanjaksot jakso-kjaksot} {})
             (map nh/harmonize-alku-and-loppu-dates jakso-kjaksot))))
    (testing
      (str "Keskeytymisajanjakso opiskeluoikeudessa, muttei jaksossa. Funktio "
           "palauttaa pelkästään opiskeluoikeuden keskeytymisajanjaksot.")
      (is (= (nh/keskeytymisajanjaksot {} opiskeluoikeus) oo-kjaksot)))
    (testing
      (str "Keskeytymisajanjaksoja sekä jaksossa että tämän "
           "opiskeluoikeudessa. Funktio palauttaa molempien kjaksot.")
      (is (= (nh/keskeytymisajanjaksot jakso-25 opiskeluoikeus)
             (concat (map nh/harmonize-alku-and-loppu-dates jakso-kjaksot)
                     oo-kjaksot))))))

(deftest test-oppijan-jaksojen-yhden-paivan-kestot
  (testing
    (str "Yhden päivän kesto jaetaan tasaisesti aktiivisena oleville "
         "jaksoille. Jos yksikään jaksoista ei ole aktiivinen, palautuu "
         "tyhjä taulu.")
    (are [y m d kestot]
         (let [oos (map :opiskeluoikeus_oid (keys kestot))]
           (= (do-rounding
                (nh/oppijan-jaksojen-yhden-paivan-kestot
                  (map nh/harmonize-alku-and-loppu-dates (keys kestot))
                  (zipmap oos (map mock-get-opiskeluoikeus-catch-404 oos))
                  (LocalDate/of y m d)))
              (map-keys nh/ids (filter-vals #(not= % 0) kestot))))
      2021 10 13  {jakso-3 0}  ; keskeytynyt (jakso)
      2021 10 14  {jakso-6 0}  ; keskeytynyt (jakso)
      2021 10 14  {jakso-10 0} ; keskeytynyt (jakso)
      2022  2  2  {jakso-25 0} ; keskeytynyt (opiskeluoikeus)
      2021 10 12  {jakso-3 1.0}
      2021  7 31  {jakso-9 1.0}
      2021 10 13  {jakso-3 0 jakso-4 1.0}  ; 3 keskeytynyt
      2021 10 12  {jakso-3 0.5 jakso-4 0.5}
      ; 2 keskeytynyt, 3 päättynyt:
      2021 10 25  {jakso-1 0.5 jakso-2 0 jakso-3 0 jakso-4 0.5}
      ; 1 ei vielä alkanut:
      2021 10 21  {jakso-1 0 jakso-2 0.33 jakso-3 0.33 jakso-4 0.33}
      ; 1 ei vielä alkanut, 11 ei vielä alkanut:
      2021  9  1
      {jakso-1 0 jakso-5 0.33 jakso-10 0.33 jakso-11 0 jakso-14 0.33}
      ; 1 ei vielä alkanut
      2021  9  9
      {jakso-1 0 jakso-5 0.25 jakso-10 0.25 jakso-11 0.25 jakso-14 0.25}
      ; 10 päättynyt, 11 päättynyt
      2021 10 22
      {jakso-1 0.33 jakso-5 0.33 jakso-10 0 jakso-11 0 jakso-14 0.33})))

(deftest test-oppijan-jaksojen-kestot
  (testing (str "Funktio palauttaa `nil`, jos jaksot sisältävässä HashMapissa "
                "ei ole jaksoja.")
    (is (= (nh/oppijan-jaksojen-kestot {} {:voi-sisaltaa-mita-tahansa nil})
           nil)))
  (testing (str "Funktio antaa jakson kestoksi nollan, mikäli "
                "osa-aikaisuustieto puuttuu tai ei ole validi.")
    (are [oa] (let [oo-oid (:opiskeluoikeus_oid jakso-2)]
                (= (nh/oppijan-jaksojen-kestot
                     [(assoc jakso-2 :osa_aikaisuus oa)]
                     {oo-oid (get opiskeluoikeudet oo-oid)})
                   {{:hoks_id 1 :yksiloiva_tunniste "2"} 0}))
      nil -1 0 101 55.5 "merkkijono" true false {}))
  (testing (str "Funktio antaa jakson kestoksi nollan, mikäli jakson "
                "opiskeluoikeus on `nil` tai sitä ei löydy "
                "`opiskeluoikeudet` hashmapista.")
    (are [opiskeluoikeudet]
         (= (nh/oppijan-jaksojen-kestot [jakso-2] opiskeluoikeudet)
            {{:hoks_id 1 :yksiloiva_tunniste "2"} 0})
      {} {"1.2.3.8" nil}))
  (testing "Yhden jakson tapauksessa jakson kesto on jakson päivien
           yhteenlaskettu lukumäärä (pois lukien keskeytymisajanjakson päivät)
           kerrottuna osa-aikaisuuskertoimella"
    (are [jakso expected-kesto]
         (let [oo-oid (:opiskeluoikeus_oid jakso)]
           (= (first (vals (nh/oppijan-jaksojen-kestot
                             [jakso] {oo-oid (get opiskeluoikeudet oo-oid)})))
              expected-kesto))
      jakso-1 2   ; 4 päivää • 0.4 = 1.6 päivää ≈ 2 päivää
      jakso-2 78  ; (107 päivää - 9 k.jakson. päivää) • 0.8 = 78.4 päivää
                  ;                                         ≈ 78 päivää
      jakso-3 9   ; (10 päivää - 1 k.jakson. päivää) • 1.0 = 9 päivää
      jakso-6 9)) ; (10 päivää - 1 k.jakson. päivää) • 1.0 = 9 päivää
  (testing
    "Useamman jakson tapauksessa kestot ovat pienempiä jyvityksen seurauksena."
    (are [jaksot expected-kestot]
         (= (vals (nh/oppijan-jaksojen-kestot
                    jaksot
                    (select-keys opiskeluoikeudet
                                 (map :opiskeluoikeus_oid jaksot))))
            expected-kestot)
      [jakso-2 jakso-3] [74 5]
      [jakso-2 jakso-4 jakso-8] [35 104 23]
      [jakso-1 jakso-4 jakso-9 jakso-16] [1 109 28 119])))

(deftest test-jaksojen-kestot!
  (with-redefs
    [ehoks/get-tyoelamajaksot-active-between!
     mock-get-tyoelamajaksot-active-between!
     koski/get-opiskeluoikeus-catch-404! mock-get-opiskeluoikeus-catch-404]
    (testing "Funktio laskee kestot oikein"
      (are [kestot] (= (nh/jaksojen-kestot! (keys kestot))
                       (map-keys nh/ids kestot))
        {jakso-2 12}
        {jakso-9 18}
        {jakso-21 16}
        {jakso-1 0 jakso-4 37}
        {jakso-9 18 jakso-21 16}
        {jakso-1 0  jakso-2 12 jakso-3 1 jakso-4 37 jakso-5 215 jakso-6 1
         jakso-7 85 jakso-8 9  jakso-9 18 jakso-10 4 jakso-11 4 jakso-12 4
         jakso-13 5 jakso-14 725 jakso-15 2 jakso-16 62 jakso-17 0
         jakso-21 16 jakso-22 25 jakso-23 0 jakso-24 14 jakso-25 13}))
    (testing (str "Lopputulos on sama kuin kestot laskettaisiin kunkin oppijan "
                  "jaksoille erikseen. Ts. funktio erottelee eri oppijoiden "
                  "jaksot kestoja laskiessaan.")
      (are [jaksot] (= (nh/jaksojen-kestot! jaksot)
                       (zipmap (map nh/ids jaksot)
                               (map #(get (nh/jaksojen-kestot! [%])
                                          (nh/ids %))
                                    jaksot)))
        [jakso-9 jakso-23]
        [jakso-3 jakso-6 jakso-15 jakso-22 jakso-23]
        jaksot-1-17
        jaksot-21-25
        jaksot-1-25))
    (testing (str "Funktio palauttaa tyhjän listan, jos eHOKSista ei saada "
                  "`get-tyoelamajaksot-active-between!`-kutsulla.")
      (with-redefs
        [ehoks/get-tyoelamajaksot-active-between! (constantly {})]
        (is (= (nh/jaksojen-kestot! jaksot-1-25) {}))))
    (testing "Kestot ovat nollia, jos Koskesta ei saada opiskeluoikeuksia."
      (with-redefs
        [koski/get-opiskeluoikeus-catch-404! (constantly nil)]
        (is (= (nh/jaksojen-kestot! jaksot-1-25)
               (zipmap (map nh/ids jaksot-1-25) (repeat 0))))))))

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
                      (str "#pvm >= :pvm AND "
                           "attribute_exists(#tunnus) AND "
                           "attribute_not_exists(mitatoity)")
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
  [{:hoks_id 1 :yksiloiva_tunniste "1" :hankkimistapa_id 1}
   {:hoks_id 1 :yksiloiva_tunniste "2" :hankkimistapa_id 2}
   {:hoks_id 1 :yksiloiva_tunniste "3" :hankkimistapa_id 3}])

(defn- mock-trauj-jaksojen-kestot [jaksot]
  (add-to-trauj-results {:type "jaksojen-kestot!"
                         :jaksot jaksot})
  {{:hoks_id 1 :yksiloiva_tunniste "1"} 1
   {:hoks_id 1 :yksiloiva_tunniste "2"} 3
   {:hoks_id 1 :yksiloiva_tunniste "3"} 5})

(defn- mock-trauj-update-jakso [jakso updates]
  (add-to-trauj-results {:type "update-jakso" :jakso jakso :updates updates}))

(deftest test-retrieve-and-update-jaksot
  (testing "Varmistaa, että retrieve-and-update-jaksot toimii oikein."
    (with-redefs
      [nh/jaksojen-kestot! mock-trauj-jaksojen-kestot
       nh/query-jaksot! mock-trauj-query-jaksot
       oph.heratepalvelu.tep.tepCommon/update-jakso mock-trauj-update-jakso]
      (let [nippu {:mock-nippu-contents "asdf"}
            results
            [{:type "query-jaksot" :nippu {:mock-nippu-contents "asdf"}}
             {:type "jaksojen-kestot!"
              :jaksot
              [{:hoks_id 1 :yksiloiva_tunniste "1" :hankkimistapa_id 1}
               {:hoks_id 1 :yksiloiva_tunniste "2" :hankkimistapa_id 2}
               {:hoks_id 1 :yksiloiva_tunniste "3" :hankkimistapa_id 3}]}
             {:type "update-jakso"
              :jakso {:hoks_id 1 :yksiloiva_tunniste "1" :hankkimistapa_id 1}
              :updates {:kesto [:n 1]}}
             {:type "update-jakso"
              :jakso {:hoks_id 1 :yksiloiva_tunniste "2" :hankkimistapa_id 2}
              :updates {:kesto [:n 3]}}
             {:type "update-jakso"
              :jakso {:hoks_id 1 :yksiloiva_tunniste "3" :hankkimistapa_id 3}
              :updates {:kesto [:n 5]}}]
            updated-jaksot
            [{:hoks_id 1 :yksiloiva_tunniste "1" :hankkimistapa_id 1 :kesto 1}
             {:hoks_id 1 :yksiloiva_tunniste "2" :hankkimistapa_id 2 :kesto 3}
             {:hoks_id 1 :yksiloiva_tunniste "3" :hankkimistapa_id 3 :kesto 5}]]
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
