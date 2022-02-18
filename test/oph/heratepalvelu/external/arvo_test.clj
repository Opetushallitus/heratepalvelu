(ns oph.heratepalvelu.external.arvo-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.external.arvo :as arvo])
  (:import (java.time LocalDate)))

(defn- mock-get-organisaatio [oid]
  (cond
    (= oid "123.123.123")
    {:oid "123.123.123"
     :tyypit ["organisaatiotyyppi_03"]}
    (= oid "456.456.456")
    {:oid "456.456.456"
     :tyypit ["organisaatiotyyppi_02"]}))

(defn- mock-get-opiskeluoikeus [oid]
  (cond
    (= oid "111.111.111")
    {:suoritukset [{:tyyppi {:koodiarvo "ammatillinentutkinto"}}]
     :koulutustoimija {:oid "111.111.111"}}
    (= oid "222.222.222")
    {:suoritukset [{:tyyppi {:koodiarvo "ammatillinentutkintoosittainen"}}]
     :koulutustoimija {:oid "222.222.222"}}
    (= oid "333.333.333")
    {:suoritukset [{:tyyppi {:koodiarvo "ammatillinentutkintoosittainen"}}]
     :koulutustoimija {:oid "333.333.333"}}))

(defn- mock-get-hankintakoulutus-oids [id]
  (cond
    (= id 1)
    []
    (= id 2)
    ["111.111.111"]
    (= id 3)
    ["222.222.222" "333.333.333"]))


(deftest test-get-toimipiste
  (testing "Get opiskeluoikeus toimipiste"
    (with-redefs [oph.heratepalvelu.external.organisaatio/get-organisaatio
                  mock-get-organisaatio]
      (let [toimipiste (arvo/get-toimipiste {:toimipiste {:oid "123.123.123"}})
            ei-toimipistetta (arvo/get-toimipiste {})
            oppilaitos (arvo/get-toimipiste {:toimipiste {:oid "456.456.456"}})]
        (is (= "123.123.123" toimipiste))
        (is (nil? ei-toimipistetta))
        (is (nil? oppilaitos))))))

(deftest test-get-osaamisalat
  (testing "Get osaamisalat"
    (with-redefs [oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 2 2))]
      (let [suoritus {:osaamisala [{:alku "2021-12-12"
                                    :loppu "2022-03-03"
                                    :koodiarvo "asdfasfdads"
                                    :osaamisala {:koodiarvo "test1"}}
                                   {:alku "2021-12-15"
                                    :koodiarvo "test2"}
                                   {:alku "2022-03-01"
                                    :loppu "2022-03-15"
                                    :koodiarvo "qrqrew"}
                                   {:alku "2021-12-31"
                                    :loppu "2022-01-25"
                                    :koodiarvo "lkhlkhjl"}]}
            expected ["test1" "test2"]]
        (is (= (arvo/get-osaamisalat suoritus "1.2.3.4") expected))
        (is (nil? (arvo/get-osaamisalat {:osaamisala []} "1.2.3.4")))))))

(deftest test-get-hankintakoulutuksen-toteuttaja
  (testing "Get hoks hankintakoulutuksen toteuttaja"
    (with-redefs [oph.heratepalvelu.external.ehoks/get-hankintakoulutus-oids
                  mock-get-hankintakoulutus-oids
                  oph.heratepalvelu.external.koski/get-opiskeluoikeus
                  mock-get-opiskeluoikeus]
      (let [ei-hankintakoulutusta (arvo/get-hankintakoulutuksen-toteuttaja 1)
            hankintakoulus-tutkinto (arvo/get-hankintakoulutuksen-toteuttaja 2)
            hankintakoulutus-tutkinnonosia
            (arvo/get-hankintakoulutuksen-toteuttaja 3)]
        (is (nil? ei-hankintakoulutusta))
        (is (= "111.111.111" hankintakoulus-tutkinto))
        (is (nil? hankintakoulutus-tutkinnonosia))))))

(deftest test-build-arvo-request-body
  (testing "Build arvo request body"
    (with-redefs
      [oph.heratepalvelu.external.arvo/get-hankintakoulutuksen-toteuttaja
       (fn [ehoks-id] (str "hkt for: " ehoks-id))
       oph.heratepalvelu.external.arvo/get-osaamisalat
       (fn [suoritus oo] (str "osaamisala: " (:oid suoritus) " " oo))
       oph.heratepalvelu.external.arvo/get-toimipiste
       (fn [suoritus] (:oid (:toimipiste suoritus)))]
      (let [herate {:alkupvm "2022-02-02"
                    :ehoks-id 123
                    :kyselytyyppi "aloittaneet"}
            opiskeluoikeus {:oid "test-oo"
                            :oppilaitos {:oid "test-laitos"}}
            request-id "test-request-id"
            koulutustoimija "test-kt"
            suoritus {:oid "test-suoritus"
                      :koulutusmoduuli {:tunniste {:koodiarvo "test-tunniste"}}
                      :suorituskieli {:koodiarvo "FI"}
                      :toimipiste {:oid "test-toimipiste"}}
            alkupvm "2022-02-16"
            loppupvm "2022-04-15"
            expected {:vastaamisajan_alkupvm "2022-02-16"
                      :heratepvm "2022-02-02"
                      :vastaamisajan_loppupvm "2022-04-15"
                      :kyselyn_tyyppi "aloittaneet"
                      :tutkintotunnus "test-tunniste"
                      :tutkinnon_suorituskieli "fi"
                      :osaamisala "osaamisala: test-suoritus test-oo"
                      :koulutustoimija_oid "test-kt"
                      :oppilaitos_oid "test-laitos"
                      :request_id "test-request-id"
                      :toimipiste_oid "test-toimipiste"
                      :hankintakoulutuksen_toteuttaja "hkt for: 123"}]
        (is (= (arvo/build-arvo-request-body herate
                                             opiskeluoikeus
                                             request-id
                                             koulutustoimija
                                             suoritus
                                             alkupvm
                                             loppupvm)
               expected))))))
