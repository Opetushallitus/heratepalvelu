(ns oph.heratepalvelu.tep.jaksoHandler-test
  (:require [clojure.test :refer :all]
            [clojure.data :refer [diff]]
            [oph.heratepalvelu.common :as c]
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

(deftest check-sort-process-keskeytymisajanjaksot-test
  (testing "sort-process-keskeytymisajanjaksot test"
    (let [herate1 {:keskeytymisajanjaksot [{:alku  "2021-08-08"
                                            :loppu "2021-08-10"}
                                           {:alku  "2021-08-01"
                                            :loppu "2021-08-04"}]}
          herate2 {:keskeytymisajanjaksot [{:alku "2021-08-08"}
                                           {:alku  "2021-08-01"
                                            :loppu "2021-08-04"}]}
          expected1 [{:alku  (LocalDate/parse "2021-08-01")
                      :loppu (LocalDate/parse "2021-08-04")}
                     {:alku  (LocalDate/parse "2021-08-08")
                      :loppu (LocalDate/parse "2021-08-10")}]
          expected2 [{:alku  (LocalDate/parse "2021-08-01")
                      :loppu (LocalDate/parse "2021-08-04")}
                     {:alku  (LocalDate/parse "2021-08-08")}]]
      (is (= expected1 (jh/sort-process-keskeytymisajanjaksot herate1)))
      (is (= expected2 (jh/sort-process-keskeytymisajanjaksot herate2))))))

(deftest test-fully-keskeytynyt?
  (testing "fully-keskeytynyt?"
    (let [herate1 {:keskeytymisajanjaksot [{:alku "2021-08-08"
                                            :loppu "2021-08-10"}
                                           {:alku "2021-08-01"
                                            :loppu "2021-08-04"}]
                   :loppupvm "2021-08-09"}
          herate2 {:keskeytymisajanjaksot [{:alku "2021-08-08"
                                            :loppu "2021-08-10"}
                                           {:alku "2021-08-01"
                                            :loppu "2021-08-04"}]
                   :loppupvm "2021-08-11"}
          herate3 {}
          herate4 {:keskeytymisajanjaksot [{:alku "2021-08-08"}]}]
      (is (jh/fully-keskeytynyt? herate1))
      (is (not (jh/fully-keskeytynyt? herate2)))
      (is (not (jh/fully-keskeytynyt? herate3)))
      (is (not (jh/fully-keskeytynyt? herate4))))))

(deftest test-has-open-keskeytymisajanjakso?
  (testing "logic for testing whether a herate has an open keskeytymisajanjakso"
    (let [herate1 {:keskeytymisajanjaksot
                   [{:alku "2021-08-08" :loppu "2021-08-10"}
                    {:alku "2021-08-01" :loppu "2021-08-04"}]}
          herate2 {}
          herate3 {:keskeytymisajanjaksot [{:alku "2021-08-08"}]}
          herate4 {:keskeytymisajanjaksot
                   [{:alku "2021-09-08"}
                    {:alku "2021-08-01" :loppu "2021-08-04"}]}]
      (is (not (jh/has-open-keskeytymisajanjakso? herate1)))
      (is (not (jh/has-open-keskeytymisajanjakso? herate2)))
      (is (jh/has-open-keskeytymisajanjakso? herate3))
      (is (jh/has-open-keskeytymisajanjakso? herate4)))))

(deftest test-osa-aikaisuus-missing?
  (testing "logic for whether a herate should be discarded because of
           osa-aikaisuus"
    (is (jh/osa-aikaisuus-missing? {:osa-aikaisuus nil :loppupvm "2023-08-01"}))
    (is (jh/osa-aikaisuus-missing? {:loppupvm "2023-08-01"}))
    (is (not (jh/osa-aikaisuus-missing?
               {:osa-aikaisuus nil :loppupvm "2023-06-30"})))
    (is (not (jh/osa-aikaisuus-missing? {:loppupvm "2023-06-30"})))
    (is (not (jh/osa-aikaisuus-missing?
               {:osa-aikaisuus 30 :loppupvm "2023-08-01"})))))

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

(defn- mock-save-to-tables-get-item [query-params table]
  (if (= (second (:ohjaaja_ytunnus_kj_tutkinto query-params)) "1")
    {}
    (if (= (second (:ohjaaja_ytunnus_kj_tutkinto query-params)) "2")
      {:kasittelytila (:ei-niputeta c/kasittelytilat)
       :sms_kasittelytila (:ei-niputeta c/kasittelytilat)}
      {:kasittelytila (:ei-lahetetty c/kasittelytilat)
       :sms_kasittelytila (:ei-lahetetty c/kasittelytilat)})))

(deftest test-save-to-tables
  (testing "Varmista, että jaksotunnus- ja nipputiedot tallennetaan oikein"
    (with-redefs
      [environ.core/env {:jaksotunnus-table "jaksotunnus-table-name"
                         :nippu-table "nippu-table-name"}
       oph.heratepalvelu.db.dynamodb/get-item mock-save-to-tables-get-item
       oph.heratepalvelu.db.dynamodb/put-item mock-save-to-tables-put-item]
      (let [jaksotunnus-table-data {:contents "jaksotunnus-table-data"}
            nippu-table-data {:contents [:s "nippu-table-data"]}
            nippu-table-data-1 {:contents [:s "nippu-table-data"]
                                :ohjaaja_ytunnus_kj_tutkinto [:s "1"]}
            nippu-table-data-2 {:contents [:s "nipput-table-data"]
                                :ohjaaja_ytunnus_kj_tutkinto [:s "2"]}
            results {:jaksotunnus-table-data jaksotunnus-table-data}
            results-1 {:jaksotunnus-table-data jaksotunnus-table-data
                       :nippu-table-data nippu-table-data-1}
            results-2 {:jaksotunnus-table-data jaksotunnus-table-data
                       :nippu-table-data nippu-table-data-2}]
        (jh/save-to-tables jaksotunnus-table-data nippu-table-data-1)
        (is (= results-1 @mock-save-to-tables-put-item-results))
        (reset! mock-save-to-tables-put-item-results {})
        (jh/save-to-tables jaksotunnus-table-data nippu-table-data-2)
        (is (= results-2 @mock-save-to-tables-put-item-results))
        (reset! mock-save-to-tables-put-item-results {})
        (jh/save-to-tables jaksotunnus-table-data nippu-table-data)
        (is (= results @mock-save-to-tables-put-item-results))))))

(def test-save-jaksotunnus-results (atom []))

(defn- add-to-test-save-jaksotunnus-results [data]
  (reset! test-save-jaksotunnus-results
          (cons data @test-save-jaksotunnus-results)))

(defn- mock-check-duplicate-hankkimistapa [tapa-id]
  (add-to-test-save-jaksotunnus-results
    {:type "mock-check-duplicate-hankkimistapa" :tapa-id tapa-id})
  true)

(defn- mock-get-toimipiste [suoritus]
  (add-to-test-save-jaksotunnus-results {:type "mock-get-toimipiste"
                                         :suoritus suoritus})
  "test-toimipiste")

(defn- mock-create-jaksotunnus [jaksotunnus-request-body]
  (add-to-test-save-jaksotunnus-results {:type "mock-create-jaksotunnus"
                                         :body (dissoc jaksotunnus-request-body
                                                       :request_id)})
  {:body {:tunnus (if (= (:tyonantaja jaksotunnus-request-body) "111111-1")
                    "DUPTNS"
                    (when (= (:tyonantaja jaksotunnus-request-body) "123456-7")
                      "ABCDEF"))}})

(defn- mock-delete-jaksotunnus [tunnus]
  (add-to-test-save-jaksotunnus-results {:type "mock-delete-jaksotunnus"
                                         :tunnus tunnus}))

(defn- mock-save-to-tables [jaksotunnus-table-data nippu-table-data]
  (add-to-test-save-jaksotunnus-results
    {:type "mock-save-to-tables"
     :jaksotunnus-table-data (dissoc jaksotunnus-table-data :request_id)
     :nippu-table-data (dissoc nippu-table-data :request_id)}))

(defn- mock-check-duplicate-tunnus [tunnus] (not= tunnus "DUPTNS"))

(deftest test-save-jaksotunnus
  (testing "Varmista, että save-jaksotunnus kutsuu funktioita oikein"
    (with-redefs
      [c/local-date-now (fn [] (LocalDate/of 2021 12 15))
       oph.heratepalvelu.external.arvo/create-jaksotunnus
       mock-create-jaksotunnus
       oph.heratepalvelu.external.arvo/delete-jaksotunnus
       mock-delete-jaksotunnus
       oph.heratepalvelu.external.arvo/get-toimipiste
       mock-get-toimipiste
       oph.heratepalvelu.tep.jaksoHandler/check-duplicate-hankkimistapa
       mock-check-duplicate-hankkimistapa
       oph.heratepalvelu.tep.jaksoHandler/check-duplicate-tunnus
       mock-check-duplicate-tunnus
       oph.heratepalvelu.tep.jaksoHandler/save-to-tables
       mock-save-to-tables]
      (let [koulutustoimija "koulutustoimija-id"
            opiskeluoikeus {:tila {:opiskeluoikeusjaksot []}
                            :suoritukset
                            [{:tyyppi {:koodiarvo "ammatillinentutkinto"}}]
                            :oppilaitos {:oid "1234"}
                            :oid "567"}
            herate-open-keskeytymisajanjakso
            {:tyopaikan-nimi "Testityöpaikka"
             :tyopaikan-ytunnus "765432-1"
             :tyopaikkaohjaaja-nimi "Testi Ojaaja"
             :alkupvm "2021-09-09"
             :loppupvm "2021-12-15"
             :hoks-id "123"
             :oppija-oid "123.456.789"
             :tyyppi "test-tyyppi"
             :tutkinnonosa-id "test-tutkinnonosa-id"
             :hankkimistapa-id 1
             :hankkimistapa-tyyppi "koulutussopimus_01"
             :keskeytymisajanjaksot [{:alku "2021-09-15"}]}
            herate-no-tunnus {:tyopaikan-nimi "Testityöpaikka"
                              :tyopaikan-ytunnus "765432-1"
                              :tyopaikkaohjaaja-nimi "Testi Ojaaja"
                              :alkupvm "2021-09-09"
                              :loppupvm "2021-12-15"
                              :hoks-id "123"
                              :oppija-oid "123.456.789"
                              :tyyppi "test-tyyppi"
                              :tutkinnonosa-id "test-tutkinnonosa-id"
                              :hankkimistapa-id 2
                              :hankkimistapa-tyyppi "koulutussopimus_01"}
            herate-duplicate-tunnus {:tyopaikan-nimi "Testityöpaikka"
                                     :tyopaikan-ytunnus "111111-1"
                                     :tyopaikkaohjaaja-nimi "Testi Ojaaja"
                                     :alkupvm "2021-09-09"
                                     :loppupvm "2021-12-15"
                                     :hoks-id "123"
                                     :oppija-oid "123.456.789"
                                     :tyyppi "test-tyyppi"
                                     :tutkinnonosa-id "test-tutkinnonosa-id"
                                     :hankkimistapa-id 3
                                     :hankkimistapa-tyyppi "koulutussopimus_01"}
            herate-good {:tyopaikan-nimi "Testityöpaikka"
                         :tyopaikan-ytunnus "123456-7"
                         :tyopaikkaohjaaja-nimi "Testi Ojaaja"
                         :alkupvm "2021-09-09"
                         :loppupvm "2021-12-15"
                         :hoks-id "123"
                         :oppija-oid "123.456.789"
                         :tyyppi "test-tyyppi"
                         :tutkinnonosa-id "test-tutkinnonosa-id"
                         :hankkimistapa-id 4
                         :hankkimistapa-tyyppi "koulutussopimus_01"
                         :keskeytymisajanjaksot []}
            results [{:type "mock-check-duplicate-hankkimistapa" :tapa-id 1}
                     {:type "mock-get-toimipiste"
                      :suoritus {:tyyppi {:koodiarvo "ammatillinentutkinto"}}}
                     {:type "mock-save-to-tables"
                      :jaksotunnus-table-data
                      {:ohjaaja_nimi [:s "Testi Ojaaja"]
                       :opiskeluoikeus_oid [:s "567"]
                       :hankkimistapa_tyyppi [:s "01"]
                       :hoks_id [:n "123"]
                       :tyopaikan_nimi [:s "Testityöpaikka"]
                       :tyopaikan_ytunnus [:s "765432-1"]
                       :jakso_loppupvm [:s "2021-12-15"]
                       :osaamisala [:s ""]
                       :tutkinnonosa_tyyppi [:s "test-tyyppi"]
                       :tpk-niputuspvm [:s "ei_maaritelty"]
                       :tallennuspvm [:s "2021-12-15"]
                       :oppilaitos [:s "1234"]
                       :ohjaaja_ytunnus_kj_tutkinto
                       [:s "Testi Ojaaja/765432-1/koulutustoimija-id/"]
                       :tutkinnonosa_id [:n "test-tutkinnonosa-id"]
                       :niputuspvm [:s "2021-12-16"]
                       :tyopaikan_normalisoitu_nimi [:s "testityopaikka"]
                       :toimipiste_oid [:s "test-toimipiste"]
                       :tutkinto [:s nil]
                       :alkupvm [:s "2021-12-16"]
                       :koulutustoimija [:s "koulutustoimija-id"]
                       :jakso_alkupvm [:s "2021-09-09"]
                       :hankkimistapa_id [:n 1]
                       :oppija_oid [:s "123.456.789"]
                       :rahoituskausi [:s "2021-2022"]
                       :tutkintonimike [:s ""]
                       :viimeinen_vastauspvm [:s "2022-02-14"]
                       :rahoitusryhma [:s "02"]}
                      :nippu-table-data
                      {:tyopaikka [:s "Testityöpaikka"]
                       :koulutuksenjarjestaja [:s "koulutustoimija-id"]
                       :ytunnus [:s "765432-1"]
                       :ohjaaja [:s "Testi Ojaaja"]
                       :ohjaaja_ytunnus_kj_tutkinto
                       [:s "Testi Ojaaja/765432-1/koulutustoimija-id/"]
                       :niputuspvm [:s "2021-12-16"]
                       :sms_kasittelytila [:s "ei_niputeta"]
                       :tutkinto [:s nil]
                       :kasittelytila [:s "ei_niputeta"]}}
                     {:type "mock-check-duplicate-hankkimistapa" :tapa-id 2}
                     {:type "mock-get-toimipiste"
                      :suoritus {:tyyppi {:koodiarvo "ammatillinentutkinto"}}}
                     {:type "mock-get-toimipiste"
                      :suoritus {:tyyppi {:koodiarvo "ammatillinentutkinto"}}}
                     {:type "mock-create-jaksotunnus"
                      :body {:osa_aikaisuus nil
                             :tyopaikka "Testityöpaikka"
                             :tyopaikka_normalisoitu "testityopaikka"
                             :vastaamisajan_alkupvm "2021-12-16"
                             :tyonantaja "765432-1"
                             :oppisopimuksen_perusta nil
                             :osaamisala nil
                             :koulutustoimija_oid "koulutustoimija-id"
                             :paikallinen_tutkinnon_osa nil
                             :tyopaikkajakson_alkupvm "2021-09-09"
                             :tyopaikkajakson_loppupvm "2021-12-15"
                             :toimipiste_oid "test-toimipiste"
                             :oppilaitos_oid "1234"
                             :tutkintotunnus nil
                             :sopimustyyppi "01"
                             :tutkintonimike ()
                             :tutkinnon_osa nil}}
                     {:type "mock-check-duplicate-hankkimistapa" :tapa-id 3}
                     {:type "mock-get-toimipiste"
                      :suoritus {:tyyppi {:koodiarvo "ammatillinentutkinto"}}}
                     {:type "mock-get-toimipiste"
                      :suoritus {:tyyppi {:koodiarvo "ammatillinentutkinto"}}}
                     {:type "mock-create-jaksotunnus"
                      :body {:osa_aikaisuus nil
                             :tyopaikka "Testityöpaikka"
                             :tyopaikka_normalisoitu "testityopaikka"
                             :vastaamisajan_alkupvm "2021-12-16"
                             :tyonantaja "111111-1"
                             :oppisopimuksen_perusta nil
                             :osaamisala nil
                             :koulutustoimija_oid "koulutustoimija-id"
                             :paikallinen_tutkinnon_osa nil
                             :tyopaikkajakson_alkupvm "2021-09-09"
                             :tyopaikkajakson_loppupvm "2021-12-15"
                             :toimipiste_oid "test-toimipiste"
                             :oppilaitos_oid "1234"
                             :tutkintotunnus nil
                             :sopimustyyppi "01"
                             :tutkintonimike ()
                             :tutkinnon_osa nil}}
                     {:type "mock-check-duplicate-hankkimistapa" :tapa-id 4}
                     {:type "mock-get-toimipiste"
                      :suoritus {:tyyppi {:koodiarvo "ammatillinentutkinto"}}}
                     {:type "mock-get-toimipiste"
                      :suoritus {:tyyppi {:koodiarvo "ammatillinentutkinto"}}}
                     {:type "mock-create-jaksotunnus"
                      :body {:osa_aikaisuus nil
                             :tyopaikka "Testityöpaikka"
                             :tyopaikka_normalisoitu "testityopaikka"
                             :vastaamisajan_alkupvm "2021-12-16"
                             :tyonantaja "123456-7"
                             :oppisopimuksen_perusta nil
                             :osaamisala nil
                             :koulutustoimija_oid "koulutustoimija-id"
                             :paikallinen_tutkinnon_osa nil
                             :tyopaikkajakson_alkupvm "2021-09-09"
                             :tyopaikkajakson_loppupvm "2021-12-15"
                             :toimipiste_oid "test-toimipiste"
                             :oppilaitos_oid "1234"
                             :tutkintotunnus nil
                             :sopimustyyppi "01"
                             :tutkintonimike ()
                             :tutkinnon_osa nil}}
                     {:type "mock-save-to-tables"
                      :jaksotunnus-table-data
                      {:ohjaaja_nimi [:s "Testi Ojaaja"]
                       :opiskeluoikeus_oid [:s "567"]
                       :hankkimistapa_tyyppi [:s "01"]
                       :hoks_id [:n "123"]
                       :tyopaikan_nimi [:s "Testityöpaikka"]
                       :tyopaikan_ytunnus [:s "123456-7"]
                       :jakso_loppupvm [:s "2021-12-15"]
                       :osaamisala [:s ""]
                       :tutkinnonosa_tyyppi [:s "test-tyyppi"]
                       :tpk-niputuspvm [:s "ei_maaritelty"]
                       :tallennuspvm [:s "2021-12-15"]
                       :oppilaitos [:s "1234"]
                       :tunnus [:s "ABCDEF"]
                       :ohjaaja_ytunnus_kj_tutkinto
                       [:s "Testi Ojaaja/123456-7/koulutustoimija-id/"]
                       :tutkinnonosa_id [:n "test-tutkinnonosa-id"]
                       :niputuspvm [:s "2021-12-16"]
                       :tyopaikan_normalisoitu_nimi [:s "testityopaikka"]
                       :toimipiste_oid [:s "test-toimipiste"]
                       :tutkinto [:s nil]
                       :alkupvm [:s "2021-12-16"]
                       :koulutustoimija [:s "koulutustoimija-id"]
                       :jakso_alkupvm [:s "2021-09-09"]
                       :hankkimistapa_id [:n 4]
                       :oppija_oid [:s "123.456.789"]
                       :rahoituskausi [:s "2021-2022"]
                       :tutkintonimike [:s ""]
                       :viimeinen_vastauspvm [:s "2022-02-14"]
                       :rahoitusryhma [:s "02"]}
                      :nippu-table-data
                      {:tyopaikka [:s "Testityöpaikka"]
                       :koulutuksenjarjestaja [:s "koulutustoimija-id"]
                       :ytunnus [:s "123456-7"]
                       :ohjaaja [:s "Testi Ojaaja"]
                       :ohjaaja_ytunnus_kj_tutkinto
                       [:s "Testi Ojaaja/123456-7/koulutustoimija-id/"]
                       :niputuspvm [:s "2021-12-16"]
                       :sms_kasittelytila [:s "ei_lahetetty"]
                       :tutkinto [:s nil]
                       :kasittelytila [:s "ei_niputettu"]}}]]
        (jh/save-jaksotunnus herate-open-keskeytymisajanjakso
                             opiskeluoikeus
                             koulutustoimija)
        (jh/save-jaksotunnus herate-no-tunnus opiskeluoikeus koulutustoimija)
        (jh/save-jaksotunnus herate-duplicate-tunnus
                             opiskeluoikeus
                             koulutustoimija)
        (jh/save-jaksotunnus herate-good opiskeluoikeus koulutustoimija)
        (is (= results (vec (reverse @test-save-jaksotunnus-results))))))))

(def test-handleJaksoHerate-results (atom []))

(defn- add-to-test-handleJaksoHerate-results [data]
  (reset! test-handleJaksoHerate-results
          (cons data @test-handleJaksoHerate-results)))

(defn- mock-get-opiskeluoikeus-catch-404 [oid]
  (add-to-test-handleJaksoHerate-results
    {:type "mock-get-opiskeluoikeus-catch-404"
     :opiskeluoikeus-oid oid})
  {:oid oid
   :koulutustoimija "mock-koulutustoimija-oid"})

(defn- mock-get-koulutustoimija-oid [opiskeluoikeus]
  (add-to-test-handleJaksoHerate-results {:type "mock-get-koulutustoimija-oid"
                                          :opiskeluoikeus opiskeluoikeus})
  (:koulutustoimija opiskeluoikeus))

(defn- mock-terminaalitilassa? [opiskeluoikeus loppupvm]
  (add-to-test-handleJaksoHerate-results
    {:type "mock-terminaalitilassa?"
     :opiskeluoikeus opiskeluoikeus
     :loppupvm loppupvm})
  false)

(defn- mock-fully-keskeytynyt? [herate]
  (add-to-test-handleJaksoHerate-results
    {:type "mock-fully-keskeytynyt?"
     :herate herate})
  false)

(defn- mock-has-one-or-more-ammatillinen-tutkinto? [opiskeluoikeus]
  (add-to-test-handleJaksoHerate-results
    {:type "mock-has-one-or-more-ammatillinen-tutkinto?"
     :opiskeluoikeus opiskeluoikeus})
  true)

(defn- mock-sisaltyy-toiseen-opiskeluoikeuteen? [opiskeluoikeus]
  (add-to-test-handleJaksoHerate-results
    {:type "mock-sisaltyy-toiseen-opiskeluoikeuteen?"
     :opiskeluoikeus opiskeluoikeus})
  false)

(defn- mock-osa-aikaisuus-missing? [herate]
  (add-to-test-handleJaksoHerate-results
    {:type "mock-osa-aikaisuus-missing?"
     :osa-aikaisuus (:osa-aikaisuus herate)})
  false)

(defn- mock-save-jaksotunnus [herate opiskeluoikeus koulutustoimija]
  (add-to-test-handleJaksoHerate-results {:type "mock-save-jaksotunnus"
                                          :herate herate
                                          :opiskeluoikeus opiskeluoikeus
                                          :koulutustoimija koulutustoimija}))

(defn- mock-patch-oht-tep-kasitelty [hankkimistapa-id]
  (add-to-test-handleJaksoHerate-results
    {:type "mock-patch-oht-tep-kasitelty"
     :hankkimistapa-id hankkimistapa-id}))

(deftest test-handleJaksoHerate
  (testing "Varmista, että -handleJaksoHerate kutsuu funktioita oikein"
    (with-redefs [c/has-one-or-more-ammatillinen-tutkinto?
                  mock-has-one-or-more-ammatillinen-tutkinto?
                  c/sisaltyy-toiseen-opiskeluoikeuteen?
                  mock-sisaltyy-toiseen-opiskeluoikeuteen?
                  c/get-koulutustoimija-oid mock-get-koulutustoimija-oid
                  c/terminaalitilassa? mock-terminaalitilassa?
                  oph.heratepalvelu.external.ehoks/patch-oht-tep-kasitelty
                  mock-patch-oht-tep-kasitelty
                  oph.heratepalvelu.external.koski/get-opiskeluoikeus-catch-404
                  mock-get-opiskeluoikeus-catch-404
                  oph.heratepalvelu.tep.jaksoHandler/osa-aikaisuus-missing?
                  mock-osa-aikaisuus-missing?
                  oph.heratepalvelu.tep.jaksoHandler/fully-keskeytynyt?
                  mock-fully-keskeytynyt?
                  oph.heratepalvelu.tep.jaksoHandler/save-jaksotunnus
                  mock-save-jaksotunnus
                  oph.heratepalvelu.tep.jaksoHandler/tep-herate-checker
                  (fn [_] nil)]
      (let [event (tu/mock-sqs-event {:opiskeluoikeus-oid "123.456.789"
                                      :loppupvm "2021-12-15"
                                      :hankkimistapa-id 12345
                                      :keskeytymisajanjaksot []})
            context (tu/mock-handler-context)
            results [{:type "mock-get-opiskeluoikeus-catch-404"
                      :opiskeluoikeus-oid "123.456.789"}
                     {:type "mock-get-koulutustoimija-oid"
                      :opiskeluoikeus
                      {:oid "123.456.789"
                       :koulutustoimija "mock-koulutustoimija-oid"}}
                     {:type "mock-terminaalitilassa?"
                      :opiskeluoikeus
                      {:oid "123.456.789"
                       :koulutustoimija "mock-koulutustoimija-oid"}
                      :loppupvm "2021-12-15"}
                     {:type "mock-osa-aikaisuus-missing?" :osa-aikaisuus nil}
                     {:type "mock-fully-keskeytynyt?"
                      :herate {:opiskeluoikeus-oid "123.456.789"
                               :loppupvm "2021-12-15"
                               :hankkimistapa-id 12345
                               :keskeytymisajanjaksot []}}
                     {:type "mock-has-one-or-more-ammatillinen-tutkinto?"
                      :opiskeluoikeus
                      {:oid "123.456.789"
                       :koulutustoimija "mock-koulutustoimija-oid"}}
                     {:type "mock-sisaltyy-toiseen-opiskeluoikeuteen?"
                      :opiskeluoikeus
                      {:oid "123.456.789"
                       :koulutustoimija "mock-koulutustoimija-oid"}}
                     {:type "mock-save-jaksotunnus"
                      :herate {:opiskeluoikeus-oid "123.456.789"
                               :loppupvm "2021-12-15"
                               :hankkimistapa-id 12345
                               :keskeytymisajanjaksot []}
                      :opiskeluoikeus
                      {:oid "123.456.789"
                       :koulutustoimija "mock-koulutustoimija-oid"}
                      :koulutustoimija "mock-koulutustoimija-oid"}
                     {:type "mock-patch-oht-tep-kasitelty"
                      :hankkimistapa-id 12345}]]
        (jh/-handleJaksoHerate {} event context)
        (is (= results (vec (reverse @test-handleJaksoHerate-results)))
            (->> @test-handleJaksoHerate-results
                 (reverse)
                 (diff results)
                 (clojure.string/join "\n")
                 (str "Differences:\n")))))))
