(ns oph.heratepalvelu.external.arvo-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.external.arvo :as arvo]))

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
    (with-redefs [oph.heratepalvelu.external.organisaatio/get-organisaatio mock-get-organisaatio]
      (let [toimipiste (arvo/get-toimipiste {:toimipiste {:oid "123.123.123"}})
            ei-toimipistetta (arvo/get-toimipiste {})
            oppilaitos (arvo/get-toimipiste {:toimipiste {:oid "456.456.456"}})]
        (is (= "123.123.123" toimipiste))
        (is (nil? ei-toimipistetta))
        (is (nil? oppilaitos))))))

(deftest test-get-hankintakoulutuksen-toteuttaja
  (testing "Get hoks hankintakoulutuksen toteuttaja"
    (with-redefs [oph.heratepalvelu.external.ehoks/get-hankintakoulutus-oids mock-get-hankintakoulutus-oids
                  oph.heratepalvelu.external.koski/get-opiskeluoikeus mock-get-opiskeluoikeus]
      (let [ei-hankintakoulutusta (arvo/get-hankintakoulutuksen-toteuttaja 1)
            hankintakoulus-tutkinto (arvo/get-hankintakoulutuksen-toteuttaja 2)
            hankintakoulutus-tutkinnonosia (arvo/get-hankintakoulutuksen-toteuttaja 3)]
        (is (nil? ei-hankintakoulutusta))
        (is (= "111.111.111" hankintakoulus-tutkinto))
        (is (nil? hankintakoulutus-tutkinnonosia))))))