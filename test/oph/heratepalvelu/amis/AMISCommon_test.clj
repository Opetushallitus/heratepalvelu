(ns oph.heratepalvelu.amis.AMISCommon-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.amis.AMISCommon :as ac]
            [oph.heratepalvelu.common :as c])
  (:import (java.time LocalDate)))

(def test-results (atom []))

(defn- add-to-test-results [data]
  (reset! test-results (cons data @test-results)))

(defn- mock-generate-uuid [] "test-uuid")

(defn- mock-check-duplicate-herate? [oppija
                                     koulutustoimija
                                     laskentakausi
                                     kyselytyyppi]
  (add-to-test-results {:type "mock-check-duplicate-herate?"
                        :oppija oppija
                        :koulutustoimija koulutustoimija
                        :laskentakausi laskentakausi
                        :kyselytyyppi kyselytyyppi})
  true)

(defn- mock-create-amis-kyselylinkki [req-body]
  (add-to-test-results {:type "mock-create-amis-kyselylinkki"
                        :req-body req-body})
  {:kysely_linkki "kysely.linkki/123"})

(defn- mock-create-amis-kyselylinkki-catch-404 [req-body]
  (add-to-test-results {:type "mock-create-amis-kyselylinkki-catch-404"
                        :req-body req-body})
  {:kysely_linkki "kysely.linkki/123"})

(defn- mock-put-item [item options]
  (add-to-test-results {:type "mock-put-item" :item item :options options}))

(defn- mock-add-kyselytunnus-to-hoks [ehoks-id data]
  (add-to-test-results {:type "mock-add-kyselytunnus-to-hoks"
                        :ehoks-id ehoks-id
                        :data data}))

(defn- mock-has-nayttotutkintoonvalmistavakoulutus? [opiskeluoikeus]
  (add-to-test-results {:type "mock-has-nayttotutkintoonvalmistavakoulutus?"
                        :opiskeluoikeus opiskeluoikeus})
  (= (:oid opiskeluoikeus) "123.456.789"))

(defn- mock-delete-amis-kyselylinkki [kyselylinkki]
  (add-to-test-results {:type "mock-delete-amis-kyselylinkki"
                        :kyselylinkki kyselylinkki}))

(defn- mock-get-toimipiste [suoritus]
  (add-to-test-results {:type "mock-get-toimipiste" :suoritus suoritus})
  "abc")

(defn- mock-get-osaamisalat [suoritus opiskeluoikeus-oid]
  (add-to-test-results {:type "mock-get-osaamisalat"
                        :suoritus suoritus
                        :opiskeluoikeus-oid opiskeluoikeus-oid})
  (seq ["a" "b" "c"]))

(defn- mock-get-hankintakoulutuksen-toteuttaja [ehoks-id]
  (add-to-test-results {:type "mock-get-hankintakoulutuksen-toteuttaja"
                        :ehoks-id ehoks-id})
  "test-hankintakoulutuksen-toteuttaja")

(deftest test-save-herate
  (testing "Varmista, että save-herate kutsuu funktioita oikein"
    (with-redefs
      [oph.heratepalvelu.common/check-duplicate-herate?
       mock-check-duplicate-herate? 
       oph.heratepalvelu.common/generate-uuid mock-generate-uuid 
       oph.heratepalvelu.common/has-nayttotutkintoonvalmistavakoulutus?
       mock-has-nayttotutkintoonvalmistavakoulutus?
       oph.heratepalvelu.common/local-date-now (fn [] (LocalDate/of 2021 12 17))
       oph.heratepalvelu.db.dynamodb/put-item mock-put-item 
       oph.heratepalvelu.external.arvo/create-amis-kyselylinkki
       mock-create-amis-kyselylinkki
       oph.heratepalvelu.external.arvo/create-amis-kyselylinkki-catch-404
       mock-create-amis-kyselylinkki-catch-404
       oph.heratepalvelu.external.arvo/get-hankintakoulutuksen-toteuttaja
       mock-get-hankintakoulutuksen-toteuttaja
       oph.heratepalvelu.external.arvo/get-osaamisalat mock-get-osaamisalat
       oph.heratepalvelu.external.arvo/get-toimipiste mock-get-toimipiste
       oph.heratepalvelu.external.ehoks/add-kyselytunnus-to-hoks
       mock-add-kyselytunnus-to-hoks]
      (let [herate-1 {:kyselytyyppi "aloittaneet"
                      :alkupvm "2021-12-15"
                      :oppija-oid "34.56.78"
                      :opiskeluoikeus-oid "123.456.789"
                      :ehoks-id 98
                      :sahkoposti "a@b.com"}
            herate-2 {:kyselytyyppi "tutkinnonsuorittaneet"
                      :alkupvm "2021-12-15"
                      :oppija-oid "56.78.34"
                      :opiskeluoikeus-oid "123.456.789"
                      :ehoks-id 98
                      :sahkoposti "a@b.com"}
            opiskeluoikeus {:oppilaitos {:oid "test-laitos-id"}
                            :oid "123.456.789"
                            :suoritukset
                            [{:tyyppi {:koodiarvo "ammatillinentutkinto"}
                              :koulutusmoduuli {:tunniste {:koodiarvo "234"}}
                              :suorituskieli {:koodiarvo "fi"}}]}
            koulutustoimija "3.4.5.6"
            results [{:type "mock-check-duplicate-herate?"
                      :oppija "34.56.78"
                      :koulutustoimija "3.4.5.6"
                      :laskentakausi "2021-2022"
                      :kyselytyyppi "aloittaneet"}
                     {:type "mock-get-osaamisalat"
                      :suoritus {:tyyppi {:koodiarvo "ammatillinentutkinto"}
                                 :koulutusmoduuli {:tunniste {:koodiarvo "234"}}
                                 :suorituskieli {:koodiarvo "fi"}}
                      :opiskeluoikeus-oid "123.456.789"}
                     {:type "mock-get-toimipiste"
                      :suoritus {:tyyppi {:koodiarvo "ammatillinentutkinto"}
                                 :koulutusmoduuli {:tunniste {:koodiarvo "234"}}
                                 :suorituskieli {:koodiarvo "fi"}}}
                     {:type "mock-get-hankintakoulutuksen-toteuttaja"
                      :ehoks-id 98}
                     {:type "mock-create-amis-kyselylinkki"
                      :req-body {:vastaamisajan_alkupvm "2021-12-17"
                                 :heratepvm "2021-12-15"
                                 :vastaamisajan_loppupvm "2022-01-15"
                                 :kyselyn_tyyppi "aloittaneet"
                                 :tutkintotunnus "234"
                                 :tutkinnon_suorituskieli "fi"
                                 :osaamisala (seq ["a" "b" "c"])
                                 :koulutustoimija_oid "3.4.5.6"
                                 :oppilaitos_oid "test-laitos-id"
                                 :request_id "test-uuid"
                                 :toimipiste_oid "abc"
                                 :hankintakoulutuksen_toteuttaja
                                 "test-hankintakoulutuksen-toteuttaja"}}
                     {:type "mock-put-item"
                      :item {:toimija_oppija [:s "3.4.5.6/34.56.78"]
                             :tyyppi_kausi [:s "aloittaneet/2021-2022"]
                             :kyselylinkki [:s "kysely.linkki/123"]
                             :sahkoposti [:s "a@b.com"]
                             :suorituskieli [:s "fi"]
                             :lahetystila [:s (:ei-lahetetty c/kasittelytilat)]
                             :alkupvm [:s "2021-12-17"]
                             :heratepvm [:s "2021-12-15"]
                             :request-id [:s "test-uuid"]
                             :oppilaitos [:s "test-laitos-id"]
                             :ehoks-id [:n "98"]
                             :opiskeluoikeus-oid [:s "123.456.789"]
                             :oppija-oid [:s "34.56.78"]
                             :koulutustoimija [:s "3.4.5.6"]
                             :kyselytyyppi [:s "aloittaneet"]
                             :rahoituskausi [:s "2021-2022"]
                             :viestintapalvelu-id [:n "-1"]
                             :voimassa-loppupvm [:s "2022-01-15"]
                             :tutkintotunnus [:s "234"]
                             :osaamisala [:s (str (seq ["a" "b" "c"]))]
                             :toimipiste-oid [:s "abc"]
                             :hankintakoulutuksen-toteuttaja
                             [:s "test-hankintakoulutuksen-toteuttaja"]
                             :tallennuspvm [:s "2021-12-17"]}
                      :options {:cond-expr
                                (str "attribute_not_exists(toimija_oppija) AND "
                                     "attribute_not_exists(tyyppi_kausi)")}}
                     {:type "mock-add-kyselytunnus-to-hoks"
                      :ehoks-id 98
                      :data {:kyselylinkki "kysely.linkki/123"
                             :tyyppi "aloittaneet"
                             :alkupvm "2021-12-17"
                             :lahetystila (:ei-lahetetty c/kasittelytilat)}}
                     {:type "mock-has-nayttotutkintoonvalmistavakoulutus?"
                      :opiskeluoikeus
                      {:oppilaitos {:oid "test-laitos-id"}
                       :oid "123.456.789"
                       :suoritukset
                       [{:tyyppi {:koodiarvo "ammatillinentutkinto"}
                         :koulutusmoduuli {:tunniste {:koodiarvo "234"}}
                         :suorituskieli {:koodiarvo "fi"}}]}}
                     {:type "mock-check-duplicate-herate?"
                      :oppija "56.78.34"
                      :koulutustoimija "3.4.5.6"
                      :laskentakausi "2021-2022"
                      :kyselytyyppi "tutkinnonsuorittaneet"}
                     {:type "mock-get-osaamisalat"
                      :suoritus {:tyyppi {:koodiarvo "ammatillinentutkinto"}
                                 :koulutusmoduuli {:tunniste {:koodiarvo "234"}}
                                 :suorituskieli {:koodiarvo "fi"}}
                      :opiskeluoikeus-oid "123.456.789"}
                     {:type "mock-get-toimipiste"
                      :suoritus {:tyyppi {:koodiarvo "ammatillinentutkinto"}
                                 :koulutusmoduuli {:tunniste {:koodiarvo "234"}}
                                 :suorituskieli {:koodiarvo "fi"}}}
                     {:type "mock-get-hankintakoulutuksen-toteuttaja"
                      :ehoks-id 98}
                     {:type "mock-create-amis-kyselylinkki-catch-404"
                      :req-body {:vastaamisajan_alkupvm "2021-12-17"
                                 :heratepvm "2021-12-15"
                                 :vastaamisajan_loppupvm "2022-01-15"
                                 :kyselyn_tyyppi "tutkinnonsuorittaneet"
                                 :tutkintotunnus "234"
                                 :tutkinnon_suorituskieli "fi"
                                 :osaamisala (seq ["a" "b" "c"])
                                 :koulutustoimija_oid "3.4.5.6"
                                 :oppilaitos_oid "test-laitos-id"
                                 :request_id "test-uuid"
                                 :toimipiste_oid "abc"
                                 :hankintakoulutuksen_toteuttaja
                                 "test-hankintakoulutuksen-toteuttaja"}}
                     {:type "mock-put-item"
                      :item {:toimija_oppija [:s "3.4.5.6/56.78.34"]
                             :tyyppi_kausi [:s "tutkinnonsuorittaneet/2021-2022"]
                             :kyselylinkki [:s "kysely.linkki/123"]
                             :sahkoposti [:s "a@b.com"]
                             :suorituskieli [:s "fi"]
                             :lahetystila [:s (:ei-lahetetty c/kasittelytilat)]
                             :alkupvm [:s "2021-12-17"]
                             :heratepvm [:s "2021-12-15"]
                             :request-id [:s "test-uuid"]
                             :oppilaitos [:s "test-laitos-id"]
                             :ehoks-id [:n "98"]
                             :opiskeluoikeus-oid [:s "123.456.789"]
                             :oppija-oid [:s "56.78.34"]
                             :koulutustoimija [:s "3.4.5.6"]
                             :kyselytyyppi [:s "tutkinnonsuorittaneet"]
                             :rahoituskausi [:s "2021-2022"]
                             :viestintapalvelu-id [:n "-1"]
                             :voimassa-loppupvm [:s "2022-01-15"]
                             :tutkintotunnus [:s "234"]
                             :osaamisala [:s (str (seq ["a" "b" "c"]))]
                             :toimipiste-oid [:s "abc"]
                             :hankintakoulutuksen-toteuttaja
                             [:s "test-hankintakoulutuksen-toteuttaja"]
                             :tallennuspvm [:s "2021-12-17"]}
                      :options {:cond-expr
                                (str "attribute_not_exists(toimija_oppija) AND "
                                     "attribute_not_exists(tyyppi_kausi)")}}
                     {:type "mock-add-kyselytunnus-to-hoks"
                      :ehoks-id 98
                      :data {:kyselylinkki "kysely.linkki/123"
                             :tyyppi "tutkinnonsuorittaneet"
                             :alkupvm "2021-12-17"
                             :lahetystila (:ei-lahetetty c/kasittelytilat)}}
                     {:type "mock-has-nayttotutkintoonvalmistavakoulutus?"
                      :opiskeluoikeus
                      {:oppilaitos {:oid "test-laitos-id"}
                       :oid "123.456.789"
                       :suoritukset
                       [{:tyyppi {:koodiarvo "ammatillinentutkinto"}
                         :koulutusmoduuli {:tunniste {:koodiarvo "234"}}
                         :suorituskieli {:koodiarvo "fi"}}]}}]]
        (ac/save-herate herate-1 opiskeluoikeus koulutustoimija)
        (ac/save-herate herate-2 opiskeluoikeus koulutustoimija)
        (is (= results (vec (reverse @test-results))))))))
