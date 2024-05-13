(ns oph.heratepalvelu.external.arvo-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.arvo :as arvo])
  (:import (clojure.lang ExceptionInfo)
           (java.time LocalDate)))

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
        (is (empty? (arvo/get-osaamisalat {:osaamisala []} "1.2.3.4")))))))

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
                      :hankintakoulutuksen_toteuttaja "hkt for: 123"
                      :metatiedot {:tila "odottaa_lahetysta"}}]
        (is (= (arvo/build-arvo-request-body herate
                                             opiskeluoikeus
                                             request-id
                                             koulutustoimija
                                             suoritus
                                             alkupvm
                                             loppupvm
                                             (-> c/kasittelytilat
                                                 :odottaa-lahetysta))
               expected))))))

(defn- mock-http [method]
  (fn [url options] {:body {:method method :url url :options options}}))

(deftest test-create-amis-kyselylinkki
  (testing "Create AMIS kyselylinkki"
    (with-redefs [environ.core/env {:arvo-url "example.com/"
                                    :arvo-user "arvo-user"}
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.http-client/post (mock-http :post)]
      (is (= (arvo/create-amis-kyselylinkki {:data "data"})
             {:method :post
              :url "example.com/vastauslinkki/v1"
              :options {:content-type "application/json"
                        :body "{\"data\":\"data\"}"
                        :basic-auth ["arvo-user" "arvo-pwd"]
                        :as :json}})))))

(deftest test-create-amis-kyselylinkki-catch-404-good
  (testing "Create AMIS kyselylinkki catch 404"
    (with-redefs [environ.core/env {:arvo-url "example.com/"
                                    :arvo-user "arvo-user"}
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.http-client/post (mock-http :post)]
      (is (= (arvo/create-amis-kyselylinkki-catch-404 {:data "data"})
             {:method :post
              :url "example.com/vastauslinkki/v1"
              :options {:content-type "application/json"
                        :body "{\"data\":\"data\"}"
                        :basic-auth ["arvo-user" "arvo-pwd"]
                        :as :json}})))))

(deftest test-create-amis-kyselylinkki-catch-404-not-found-error
  (testing "Create AMIS kyselylinkki catch 404 with 404 exeption"
    (with-redefs [oph.heratepalvelu.external.arvo/create-amis-kyselylinkki
                  (fn [_] (throw (ex-info "Test" {:status 404})))]
      (is (nil? (arvo/create-amis-kyselylinkki-catch-404 {:data "data"}))))))

(deftest test-create-amis-kyselylinkki-catch-404-other-error
  (testing "Create AMIS kyselylinkki catch 404 with other exeption"
    (with-redefs [oph.heratepalvelu.external.arvo/create-amis-kyselylinkki
                  (fn [data] (throw (ex-info "Test" {:data data})))]
      (is (thrown-with-msg?
            ExceptionInfo
            #"Test"
            (arvo/create-amis-kyselylinkki-catch-404 {:data "data"}))))))

(deftest test-delete-amis-kyselylinkki
  (testing "Delete AMIS kyselylinkki"
    (with-redefs [environ.core/env {:arvo-url "example.com/"
                                    :arvo-user "arvo-user"}
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.http-client/delete (mock-http
                                                                  :delete)]
      (is (= (arvo/delete-amis-kyselylinkki "kysely.linkki/123")
             {:body {:method :delete
                     :url "example.com/vastauslinkki/v1/123"
                     :options {:basic-auth ["arvo-user" "arvo-pwd"]}}})))))

(deftest test-get-kyselylinkki-status
  (testing "Get kyselylinkki status"
    (with-redefs [environ.core/env {:arvo-url "example.com/"
                                    :arvo-user "arvo-user"}
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.http-client/get (mock-http :get)]
      (is (= (arvo/get-kyselylinkki-status "kysely.linkki/123")
             {:method :get
              :url "example.com/vastauslinkki/v1/status/123"
              :options {:basic-auth ["arvo-user" "arvo-pwd"] :as :json}})))))

(deftest test-get-nippulinkki-status
  (testing "Get nippulinkki status"
    (with-redefs [environ.core/env {:arvo-url "example.com/"
                                    :arvo-user "arvo-user"}
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.http-client/get (mock-http :get)]
      (is (= (arvo/get-nippulinkki-status "kysely.linkki/123")
             {:method :get
              :url "example.com/tyoelamapalaute/v1/status/123"
              :options {:basic-auth ["arvo-user" "arvo-pwd"] :as :json}})))))

(deftest test-patch-kyselylinkki-metadata
  (testing "Patch kyselylinkki metadata"
    (with-redefs [environ.core/env {:arvo-url "example.com/"
                                    :arvo-user "arvo-user"}
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.http-client/patch (mock-http
                                                                 :patch)]
      (is (= (arvo/patch-kyselylinkki-metadata "kysely.linkki/123" "test-tila")
             {:method :patch
              :url "example.com/vastauslinkki/v1/123/metatiedot"
              :options {:basic-auth ["arvo-user" "arvo-pwd"]
                        :content-type "application/json"
                        :body "{\"tila\":\"test-tila\"}"
                        :as :json}})))))

(deftest test-build-jaksotunnus-request-body
  (testing "Build jaksotunnus request body"
    (with-redefs [oph.heratepalvelu.external.arvo/get-osaamisalat
                  (fn [suoritus oo] (str "osaamisala: " (:oid suoritus) " " oo))
                  oph.heratepalvelu.external.arvo/get-toimipiste
                  (fn [suoritus] (:oid (:toimipiste suoritus)))]
      (let [herate {:tyopaikan-ytunnus "123456-7"
                    :tyopaikan-nimi "Työ Paikka"
                    :tutkinnonosa-koodi "asdf_oiuoiu_lkj"
                    :tutkinnonosa-nimi "LKJ"
                    :alkupvm "2022-01-01"
                    :loppupvm "2022-03-01"
                    :osa-aikaisuus 90
                    :hankkimistapa-tyyppi "osaamisenhankkimistapa_oppisopimus"
                    :oppisopimuksen-perusta "oppisopimuksenperusta_01"}
            tyopaikka-normalisoitu "tyo_paikka"
            opiskeluoikeus {:oid "test-oo"
                            :oppilaitos {:oid "test-laitos"}}
            request-id "test-request-id"
            koulutustoimija "test-kt"
            suoritus {:oid "test-suoritus"
                      :koulutusmoduuli {:tunniste {:koodiarvo "test-tunniste"}}
                      :tutkintonimike [{:koodiarvo "test-tutkintonimike"}]
                      :toimipiste {:oid "test-toimipiste"}}
            niputuspvm "2022-03-16"]
        (is (= (arvo/build-jaksotunnus-request-body herate
                                                    tyopaikka-normalisoitu
                                                    opiskeluoikeus
                                                    request-id
                                                    koulutustoimija
                                                    suoritus
                                                    niputuspvm)
               {:koulutustoimija_oid "test-kt"
                :tyonantaja "123456-7"
                :tyopaikka "Työ Paikka"
                :tyopaikka_normalisoitu "tyo_paikka"
                :tutkintotunnus "test-tunniste"
                :tutkinnon_osa "lkj"
                :paikallinen_tutkinnon_osa "LKJ"
                :tutkintonimike (seq ["test-tutkintonimike"])
                :osaamisala "osaamisala: test-suoritus test-oo"
                :tyopaikkajakson_alkupvm "2022-01-01"
                :tyopaikkajakson_loppupvm "2022-03-01"
                :osa_aikaisuus 90
                :sopimustyyppi "oppisopimus"
                :oppisopimuksen_perusta "01"
                :vastaamisajan_alkupvm "2022-03-16"
                :oppilaitos_oid "test-laitos"
                :toimipiste_oid "test-toimipiste"
                :request_id "test-request-id"}))))))

(deftest test-create-jaksotunnus
  (testing "Create jaksotunnus"
    (with-redefs [environ.core/env {:arvo-url "example.com/"
                                    :arvo-user "arvo-user"}
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.http-client/post (mock-http :post)]
      (is (= (arvo/create-jaksotunnus {:data "data"})
             {:body {:method :post
                     :url "example.com/tyoelamapalaute/v1/vastaajatunnus"
                     :options {:content-type "application/json"
                               :body "{\"data\":\"data\"}"
                               :basic-auth ["arvo-user" "arvo-pwd"]
                               :as :json}}})))))

(deftest test-create-jaksotunnus-not-found-error
  (testing "Create jaksotunnus with 404 exeption"
    (with-redefs [environ.core/env {:arvo-url "example.com/"
                                    :arvo-user "arvo-user"}
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.http-client/post
                  (fn [_ _] (throw (ex-info "Test" {:status 404})))]
      (is (nil? (arvo/create-jaksotunnus {:data "data"}))))))

(deftest test-create-jaksotunnus-other-error
  (testing "Create jaksotunnus with other exeption"
    (with-redefs [environ.core/env {:arvo-url "example.com/"
                                    :arvo-user "arvo-user"}
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.http-client/post
                  (fn [_ _] (throw (ex-info "Test" {})))]
      (is (thrown-with-msg? ExceptionInfo
                            #"Test"
                            (arvo/create-jaksotunnus {:data "data"}))))))

(deftest test-delete-jaksotunnus
  (testing "Delete jaksotunnus"
    (with-redefs [environ.core/env {:arvo-url "example.com/"
                                    :arvo-user "arvo-user"}
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.http-client/delete (mock-http
                                                                  :delete)]
      (is (= (arvo/delete-jaksotunnus "1234")
             {:body {:method :delete
                     :url "example.com/tyoelamapalaute/v1/vastaajatunnus/1234"
                     :options {:basic-auth ["arvo-user" "arvo-pwd"]}}})))))

(deftest test-build-niputus-request-body
  (testing "Build niputus request body"
    (with-redefs [oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 2 2))]
      (let [tunniste "test-tunniste"
            nippu {:tyopaikka "Työ Paikka"
                   :ytunnus "123456-7"
                   :koulutuksenjarjestaja "test-kj"
                   :tutkinto "test-tutkinto"}
            tunnukset ["ASDF" "HJKL"]
            request-id "test-request-id"]
        (is (= (arvo/build-niputus-request-body tunniste
                                                nippu
                                                tunnukset
                                                request-id
                                                "2024-05-13")
               {:tunniste "test-tunniste"
                :koulutustoimija_oid "test-kj"
                :tutkintotunnus "test-tutkinto"
                :tyonantaja "123456-7"
                :tyopaikka "Työ Paikka"
                :tunnukset ["ASDF" "HJKL"]
                :voimassa_alkupvm "2022-02-02"
                :request_id "test-request-id"
                :rahoituskausi_pvm "2024-05-13"}))))))

(deftest test-create-nippu-kyselylinkki
  (testing "Create nippu kyselylinkki"
    (with-redefs [environ.core/env {:arvo-url "example.com/"
                                    :arvo-user "arvo-user"}
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.http-client/post (mock-http :post)]
      (is (= (arvo/create-nippu-kyselylinkki {:data "data"})
             {:method :post
              :url "example.com/tyoelamapalaute/v1/nippu"
              :options {:content-type "application/json"
                        :body "{\"data\":\"data\"}"
                        :basic-auth ["arvo-user" "arvo-pwd"]
                        :as :json}})))))

(deftest test-create-nippu-kyselylinkki-not-found-error
  (testing "Create nippu kyselylinkki with 404 exeption"
    (with-redefs [environ.core/env {:arvo-url "example.com/"
                                    :arvo-user "arvo-user"}
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.http-client/post
                  (fn [_ _] (throw (ex-info "Test" {:status 404})))]
      (is (nil? (arvo/create-nippu-kyselylinkki {:data "data"}))))))

(deftest test-create-nippu-kyselylinkki-other-error
  (testing "Create nippu kyselylinkki with other exeption"
    (with-redefs [environ.core/env {:arvo-url "example.com/"
                                    :arvo-user "arvo-user"}
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.http-client/post
                  (fn [_ _] (throw (ex-info "Test" {})))]
      (is (thrown-with-msg? ExceptionInfo
                            #"Test"
                            (arvo/create-nippu-kyselylinkki {:data "data"}))))))

(deftest test-delete-nippukyselylinkki
  (testing "Delete nippukyselylinkki"
    (with-redefs [environ.core/env {:arvo-url "example.com/"
                                    :arvo-user "arvo-user"}
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.http-client/delete (mock-http
                                                                  :delete)]
      (is (= (arvo/delete-nippukyselylinkki "1234")
             {:body {:method :delete
                     :url "example.com/tyoelamapalaute/v1/nippu/1234"
                     :options {:basic-auth ["arvo-user" "arvo-pwd"]}}})))))

(deftest test-patch-nippulinkki
  (testing "Patch nippulinkki"
    (with-redefs [environ.core/env {:arvo-url "example.com/"
                                    :arvo-user "arvo-user"}
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.http-client/patch (mock-http
                                                                 :patch)]
      (is (= (arvo/patch-nippulinkki "kysely.linkki/123" {:data "data"})
             {:method :patch
              :url "example.com/tyoelamapalaute/v1/nippu/123"
              :options {:basic-auth ["arvo-user" "arvo-pwd"]
                        :content-type "application/json"
                        :body "{\"data\":\"data\"}"
                        :as :json}})))))

(deftest test-patch-vastaajatunnus
  (testing "Patch vastaajatunnus"
    (with-redefs [environ.core/env {:arvo-url "example.com/"
                                    :arvo-user "arvo-user"}
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.http-client/patch (mock-http
                                                                 :patch)]
      (is (= (arvo/patch-vastaajatunnus "123" {:data "data"})
             {:body {:method :patch
                     :url "example.com/tyoelamapalaute/v1/vastaajatunnus/123"
                     :options {:basic-auth ["arvo-user" "arvo-pwd"]
                               :content-type "application/json"
                               :body "{\"data\":\"data\"}"
                               :as :json}}})))))

(deftest test-build-tpk-request-body
  (testing "Build TPK request body"
    (let [nippu {:tyopaikan-nimi "Työ Paikka"
                 :tyopaikan-nimi-normalisoitu "tyo_paikka"
                 :tyopaikan-ytunnus "123456-7"
                 :vastaamisajan-alkupvm "2022-01-15"
                 :vastaamisajan-loppupvm "2022-02-28"
                 :koulutustoimija-oid "test-kt"
                 :tiedonkeruu-alkupvm "2021-07-01"
                 :tiedonkeruu-loppupvm "2021-12-31"
                 :request-id "test-request-id"}]
      (is (= (arvo/build-tpk-request-body nippu)
             {:tyopaikka "Työ Paikka"
              :tyopaikka_normalisoitu "tyo_paikka"
              :tyonantaja "123456-7"
              :vastaamisajan_alkupvm "2022-01-15"
              :vastaamisajan_loppupvm "2022-02-28"
              :koulutustoimija_oid "test-kt"
              :tiedonkeruu_alkupvm "2021-07-01"
              :tiedonkeruu_loppupvm "2021-12-31"
              :request_id "test-request-id"})))))

(deftest test-create-tpk-kyselylinkki
  (testing "Create TPK kyselylinkki"
    (with-redefs [environ.core/env {:arvo-url "example.com/"
                                    :arvo-user "arvo-user"}
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.http-client/post (mock-http :post)]
      (is (= (arvo/create-tpk-kyselylinkki {:data "data"})
             {:method :post
              :url "example.com/tyoelamapalaute/v1/tyopaikkakysely-tunnus"
              :options {:content-type "application/json"
                        :body "{\"data\":\"data\"}"
                        :basic-auth ["arvo-user" "arvo-pwd"]
                        :as :json}})))))
