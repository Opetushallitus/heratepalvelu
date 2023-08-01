(ns oph.heratepalvelu.integration-tests.amis.AMISherateEmailHandler-i-test
  (:require [clojure.test :refer :all]
            [clojure.data :refer [diff]]
            [oph.heratepalvelu.amis.AMISherateEmailHandler :as heh]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.integration-tests.mock-cas-client :as mcc]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.integration-tests.mock-http-client :as mhc]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(def mock-env {:herate-table "herate-table-name"
               :viestintapalvelu-url "viestintapalvelu-example.com"
               :ehoks-url "ehoks-example.com/"
               :arvo-url "arvo-example.com/"
               :arvo-user "arvo-user"
               :organisaatio-url "organisaatio-example.com/"
               :koski-url "koski-example.com"
               :koski-user "koski-user"})

(def starting-table-contents
  [{:toimija_oppija [:s "abc/123"]
    :tyyppi_kausi [:s "aloittaneet/2021-2022"]
    :muistutukset [:n 2]
    :kyselylinkki [:s "kysely.linkki/123"]
    :sahkoposti [:s "sahko.posti@esimerkki.fi"]
    :lahetystila [:s (:ei-lahetetty c/kasittelytilat)]
    :sms-lahetystila [:s (:ei-lahetetty c/kasittelytilat)]
    :suorituskieli [:s "fi"]
    :kyselytyyppi [:s "aloittaneet"]
    :alkupvm [:s "2022-01-01"]
    :heratepvm [:s "2022-01-01"]}
   {:toimija_oppija [:s "lkj/245"]
    :tyyppi_kausi [:s "paattyneet/2022-2023"]
    :kyselylinkki [:s "kysely.linkki/245"]
    :sahkoposti [:s "asdf@esimerkki.fi"]
    :lahetystila [:s (:ei-lahetetty c/kasittelytilat)]
    :sms-lahetystila [:s (:ei-lahetetty c/kasittelytilat)]
    :suorituskieli [:s "fi"]
    :kyselytyyppi [:s "tutkinnon_suorittaneet"]
    :alkupvm [:s "2022-01-05"]
    :heratepvm [:s "2022-01-05"]}
   {:toimija_oppija [:s "test/1"]
    :tyyppi_kausi [:s "asdf"]
    :lahetystila [:s (:success c/kasittelytilat)]
    :sms-lahetystila [:s (:ei-lahetetty c/kasittelytilat)]
    :alkupvm [:s "2022-06-06"]
    :heratepvm [:s "2022-06-06"]}
   {:toimija_oppija [:s "test/2"]
    :tyyppi_kausi [:s "asdf"]
    :lahetystila [:s (:success c/kasittelytilat)]
    :sms-lahetystila [:s (:ei-lahetetty c/kasittelytilat)]
    :alkupvm [:s "2022-06-04"]
    :heratepvm [:s "2022-06-04"]}
   {:toimija_oppija [:s "abc/345"]
    :tyyppi_kausi [:s "aloittaneet/2021-2022"]
    :sahkoposti [:s "sahko.posti2@esimerkki.fi"]
    :lahetystila [:s (:ei-lahetetty c/kasittelytilat)]
    :sms-lahetystila [:s (:ei-lahetetty c/kasittelytilat)]
    :suorituskieli [:s "fi"]
    :kyselytyyppi [:s "aloittaneet"]
    :alkupvm [:s "2022-01-15"]
    :heratepvm [:s "2022-01-15"]
    :opiskeluoikeus-oid [:s "1234"]}
   {:toimija_oppija [:s "abc/346"]
    :tyyppi_kausi [:s "aloittaneet/2021-2022"]
    :sahkoposti [:s "sahko.posti3@esimerkki.fi"]
    :lahetystila [:s (:ei-lahetetty c/kasittelytilat)]
    :sms-lahetystila [:s (:ei-lahetetty c/kasittelytilat)]
    :suorituskieli [:s "fi"]
    :kyselytyyppi [:s "aloittaneet"]
    :alkupvm [:s "2022-01-15"]
    :heratepvm [:s "2022-01-15"]
    :opiskeluoikeus-oid [:s "1241"]}
   {:toimija_oppija [:s "abc/347"]
    :ehoks-id [:n 189438]
    :tyyppi_kausi [:s "aloittaneet/2021-2022"]
    :sahkoposti [:s "sahko.posti4@esimerkki.fi"]
    :lahetystila [:s (:ei-lahetetty c/kasittelytilat)]
    :sms-lahetystila [:s (:ei-lahetetty c/kasittelytilat)]
    :suorituskieli [:s "fi"]
    :kyselytyyppi [:s "aloittaneet"]
    :alkupvm [:s "2022-01-15"]
    :heratepvm [:s "2022-01-15"]
    :opiskeluoikeus-oid [:s "1242"]}
   {:toimija_oppija [:s "abc/348"]
    :ehoks-id [:n 189439]
    :tyyppi_kausi [:s "aloittaneet/2021-2022"]
    :sahkoposti [:s "sahko.posti5@esimerkki.fi"]
    :lahetystila [:s (:ei-lahetetty c/kasittelytilat)]
    :sms-lahetystila [:s (:ei-lahetetty c/kasittelytilat)]
    :suorituskieli [:s "fi"]
    :kyselytyyppi [:s "tutkinnon_suorittaneet"]
    :alkupvm [:s "2022-01-15"]
    :heratepvm [:s "2022-01-15"]
    :opiskeluoikeus-oid [:s "1243"]}])

(defn- setup-test []
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mhc/bind-url :get
                (str (:arvo-url mock-env) "vastauslinkki/v1/status/123")
                {:basic-auth [(:arvo-user mock-env) "arvo-pwd"] :as :json}
                {:body {:voimassa_loppupvm "2022-02-28"}})
  (mhc/bind-url :get
                (str (:arvo-url mock-env) "vastauslinkki/v1/status/245")
                {:basic-auth [(:arvo-user mock-env) "arvo-pwd"] :as :json}
                {:body {:voimassa_loppupvm "2022-02-01"}})
  (mhc/bind-url :get
                (str (:arvo-url mock-env) "vastauslinkki/v1/status/86423")
                {:basic-auth [(:arvo-user mock-env) "arvo-pwd"] :as :json}
                {:body {:voimassa_loppupvm "2022-01-28"}})
  (mhc/sloppy-bind-url :post
                       (str (:arvo-url mock-env) "vastauslinkki/v1")
                       {:body {:kysely_linkki "kysely.linkki/86423"}})
  (mhc/bind-url :get
                (str (:koski-url mock-env) "/opiskeluoikeus/1234")
                {:basic-auth [(:koski-user mock-env) "koski-pwd"] :as :json}
                {:body {:oid "1234"
                        :tila {:opiskeluoikeusjaksot
                               [{:alku "2022-01-10"
                                 :tila {:koodiarvo "lasna"}
                                 :opintojenRahoitus {:koodiarvo "14"}}
                                {:alku "2022-01-05"
                                 :tila {:koodiarvo "lasna"}
                                 :opintojenRahoitus {:koodiarvo "1"}}]}}})
  (mhc/bind-url :get
                (str (:koski-url mock-env) "/opiskeluoikeus/1242")
                {:basic-auth [(:koski-user mock-env) "koski-pwd"] :as :json}
                {:body {:oid "1242"
                        :suoritukset
                        [{:suorituskieli {:koodiarvo "FI"}
                          :tyyppi {:koodiarvo "ammatillinentutkintoosittainen"}
                          :vahvistus {:päivä "2019-07-24"}}]
                        :tila {:opiskeluoikeusjaksot
                               [{:alku "2022-01-10"
                                 :tila {:koodiarvo "lasna"}
                                 :opintojenRahoitus {:koodiarvo "1"}}
                                {:alku "2022-01-05"
                                 :tila {:koodiarvo "lasna"}
                                 :opintojenRahoitus {:koodiarvo "14"}}]}}})
  (mhc/bind-url :get
                (str (:koski-url mock-env) "/opiskeluoikeus/1243")
                {:basic-auth [(:koski-user mock-env) "koski-pwd"] :as :json}
                {:body {:oid "1243"
                        :tila {:opiskeluoikeusjaksot
                               [{:alku "2022-01-10"
                                 :tila {:koodiarvo "eronnut"}
                                 :opintojenRahoitus {:koodiarvo "1"}}]}}})
  (mcc/bind-url :post
                (:viestintapalvelu-url mock-env)
                {:recipient [{:email "sahko.posti@esimerkki.fi"}]
                 :email {:callingProcess "heratepalvelu"
                         :from "no-reply@opintopolku.fi"
                         :sender "Opetushallitus – Utbildningsstyrelsen – EDUFI"
                         :subject (str "Palautetta oppilaitokselle - "
                                       "Respons till läroanstalten - "
                                       "Feedback to educational institution")
                         :isHtml true
                         :body (vp/amispalaute-html
                                 {:suorituskieli "fi"
                                  :kyselylinkki "kysely.linkki/123"
                                  :kyselytyyppi "aloittaneet"})}}
                {:body {:id 123}})
  (mdb/clear-mock-db)
  (mdb/create-table (:herate-table mock-env) {:primary-key :toimija_oppija
                                              :sort-key :tyyppi_kausi})
  (mdb/add-index-key-fields (:herate-table mock-env)
                            "lahetysIndex"
                            {:primary-key :lahetystila
                             :sort-key :alkupvm})
  (mdb/set-table-contents (:herate-table mock-env) starting-table-contents))

(defn- teardown-test []
  (mcc/clear-results)
  (mcc/clear-url-bindings)
  (mdb/clear-mock-db))

(def expected-table-contents
  #{{:toimija_oppija [:s "abc/123"]
     :tyyppi_kausi [:s "aloittaneet/2021-2022"]
     :muistutukset [:n 0]
     :kyselylinkki [:s "kysely.linkki/123"]
     :sahkoposti [:s "sahko.posti@esimerkki.fi"]
     :lahetystila [:s (:viestintapalvelussa c/kasittelytilat)]
     :sms-lahetystila [:s (:ei-lahetetty c/kasittelytilat)]
     :lahetyspvm [:s "2022-02-02"]
     :suorituskieli [:s "fi"]
     :kyselytyyppi [:s "aloittaneet"]
     :alkupvm [:s "2022-01-01"]
     :heratepvm [:s "2022-01-01"]
     :viestintapalvelu-id [:n 123]}
    {:toimija_oppija [:s "lkj/245"]
     :tyyppi_kausi [:s "paattyneet/2022-2023"]
     :kyselylinkki [:s "kysely.linkki/245"]
     :sahkoposti [:s "asdf@esimerkki.fi"]
     :lahetystila [:s (:vastausaika-loppunut c/kasittelytilat)]
     :sms-lahetystila [:s (:ei-lahetetty c/kasittelytilat)]
     :lahetyspvm [:s "2022-02-02"]
     :suorituskieli [:s "fi"]
     :kyselytyyppi [:s "tutkinnon_suorittaneet"]
     :alkupvm [:s "2022-01-05"]
     :heratepvm [:s "2022-01-05"]}
    {:toimija_oppija [:s "test/1"]
     :tyyppi_kausi [:s "asdf"]
     :lahetystila [:s (:success c/kasittelytilat)]
     :sms-lahetystila [:s (:ei-lahetetty c/kasittelytilat)]
     :alkupvm [:s "2022-06-06"]
     :heratepvm [:s "2022-06-06"]}
    {:toimija_oppija [:s "test/2"]
     :tyyppi_kausi [:s "asdf"]
     :lahetystila [:s (:success c/kasittelytilat)]
     :sms-lahetystila [:s (:ei-lahetetty c/kasittelytilat)]
     :alkupvm [:s "2022-06-04"]
     :heratepvm [:s "2022-06-04"]}
    {:toimija_oppija [:s "abc/345"]
     :tyyppi_kausi [:s "aloittaneet/2021-2022"]
     :sahkoposti [:s "sahko.posti2@esimerkki.fi"]
     :lahetystila [:s (:ei-laheteta c/kasittelytilat)]
     :sms-lahetystila [:s (:ei-laheteta c/kasittelytilat)]
     :suorituskieli [:s "fi"]
     :kyselytyyppi [:s "aloittaneet"]
     :alkupvm [:s "2022-01-15"]
     :heratepvm [:s "2022-01-15"]
     :opiskeluoikeus-oid [:s "1234"]}
    {:toimija_oppija [:s "abc/346"]
     :tyyppi_kausi [:s "aloittaneet/2021-2022"]
     :sahkoposti [:s "sahko.posti3@esimerkki.fi"]
     :lahetystila [:s (:ei-laheteta-oo-ei-loydy c/kasittelytilat)]
     :sms-lahetystila [:s (:ei-laheteta-oo-ei-loydy c/kasittelytilat)]
     :suorituskieli [:s "fi"]
     :kyselytyyppi [:s "aloittaneet"]
     :alkupvm [:s "2022-01-15"]
     :heratepvm [:s "2022-01-15"]
     :opiskeluoikeus-oid [:s "1241"]}
    {:toimija_oppija [:s "abc/347"]
     :ehoks-id [:n 189438]
     :tyyppi_kausi [:s "aloittaneet/2021-2022"]
     :sahkoposti [:s "sahko.posti4@esimerkki.fi"]
     :lahetystila [:s (:vastausaika-loppunut c/kasittelytilat)]
     :sms-lahetystila [:s (:ei-lahetetty c/kasittelytilat)]
     :lahetyspvm [:s "2022-02-02"]
     :suorituskieli [:s "fi"]
     :kyselytyyppi [:s "aloittaneet"]
     :kyselylinkki [:s "kysely.linkki/86423"]
     :alkupvm [:s "2022-01-15"]
     :heratepvm [:s "2022-01-15"]
     :opiskeluoikeus-oid [:s "1242"]}
    {:kyselytyyppi [:s "tutkinnon_suorittaneet"],
     :suorituskieli [:s "fi"],
     :sahkoposti [:s "sahko.posti5@esimerkki.fi"],
     :toimija_oppija [:s "abc/348"],
     :heratepvm [:s "2022-01-15"],
     :lahetystila [:s "ei_laheteta"],
     :tyyppi_kausi [:s "aloittaneet/2021-2022"],
     :opiskeluoikeus-oid [:s "1243"],
     :alkupvm [:s "2022-01-15"],
     :sms-lahetystila [:s "ei_laheteta"],
     :ehoks-id [:n 189439]}})

(def expected-http-results
  [{:method :get
    :url "arvo-example.com/vastauslinkki/v1/status/123"
    :options {:basic-auth ["arvo-user" "arvo-pwd"]
              :as :json}}
   {:method :patch
    :url "ehoks-example.com/hoks/kyselylinkki"
    :options {:headers {:ticket (str "service-ticket/ehoks-virkailija-backend"
                                     "/cas-security-check")}
              :content-type "application/json"
              :body (str "{\"kyselylinkki\":\"kysely.linkki/123\","
                         "\"lahetyspvm\":\"2022-02-02\","
                         "\"sahkoposti\":\"sahko.posti@esimerkki.fi\","
                         "\"lahetystila\":\"viestintapalvelussa\"}")
              :as :json}}
   {:method :get
    :url "arvo-example.com/vastauslinkki/v1/status/245"
    :options {:basic-auth ["arvo-user" "arvo-pwd"] :as :json}}
   {:method :get
    :url "koski-example.com/opiskeluoikeus/1234"
    :options {:basic-auth ["koski-user" "koski-pwd"] :as :json}}
   {:method :get
    :url "koski-example.com/opiskeluoikeus/1241"
    :options {:basic-auth ["koski-user" "koski-pwd"] :as :json}}
   {:method :get
    :url "koski-example.com/opiskeluoikeus/1242"
    :options {:basic-auth ["koski-user" "koski-pwd"] :as :json}}
   {:method :get :url "organisaatio-example.com/" :options {:as :json}}
   {:method :get :url "ehoks-example.com/hoks/189438/hankintakoulutukset"
    :options
    {:headers
     {:ticket "service-ticket/ehoks-virkailija-backend/cas-security-check"}
     :as :json}}
   {:method :post
    :url "arvo-example.com/vastauslinkki/v1"
    :options
    {:content-type "application/json"
     :body (str "{\"vastaamisajan_alkupvm\":\"2022-01-15\","
                "\"osaamisala\":[],\"heratepvm\":\"2022-01-15\","
                "\"koulutustoimija_oid\":null,"
                "\"tutkinnon_suorituskieli\":\"fi\",\"toimipiste_oid\":null,"
                "\"oppilaitos_oid\":null,"
                "\"hankintakoulutuksen_toteuttaja\":null,"
                "\"kyselyn_tyyppi\":\"aloittaneet\",\"tutkintotunnus\":null,"
                "\"request_id\":null,\"vastaamisajan_loppupvm\":null}")
     :basic-auth ["arvo-user" "arvo-pwd"], :as :json}}
   {:method :post
    :url "ehoks-example.com/hoks/189438/kyselylinkki"
    :options
    {:headers
     {:ticket "service-ticket/ehoks-virkailija-backend/cas-security-check"}
     :as :json
     :content-type "application/json"
     :body (str "{\"kyselylinkki\":\"kysely.linkki/86423\","
                "\"tyyppi\":\"aloittaneet\",\"alkupvm\":\"2022-01-15\","
                "\"lahetystila\":\"ei_lahetetty\"}")}}
   {:method :get
    :url "arvo-example.com/vastauslinkki/v1/status/86423"
    :options {:basic-auth ["arvo-user" "arvo-pwd"], :as :json}}
   {:method :get,
    :url "koski-example.com/opiskeluoikeus/1243",
    :options {:basic-auth ["koski-user" "koski-pwd"], :as :json}}])

(def expected-cas-client-results
  [{:method :post
    :url (:viestintapalvelu-url mock-env)
    :body {:recipient [{:email "sahko.posti@esimerkki.fi"}]
           :email {:callingProcess "heratepalvelu"
                   :from "no-reply@opintopolku.fi"
                   :sender "Opetushallitus – Utbildningsstyrelsen – EDUFI"
                   :subject (str "Palautetta oppilaitokselle - "
                                 "Respons till läroanstalten - "
                                 "Feedback to educational institution")
                   :isHtml true
                   :body (vp/amispalaute-html {:suorituskieli "fi"
                                               :kyselylinkki "kysely.linkki/123"
                                               :kyselytyyppi "aloittaneet"})}}
    :options {:as :json}}
   {:type :get-service-ticket
    :service "/ehoks-virkailija-backend"
    :suffix "cas-security-check"}
   {:type :get-service-ticket
    :service "/ehoks-virkailija-backend"
    :suffix "cas-security-check"}
   {:type :get-service-ticket
    :service "/ehoks-virkailija-backend"
    :suffix "cas-security-check"}])

(deftest test-AMISherateEmailHandler-integration
  (testing "AMISherateEmailHandler integraatiotesti"
    (with-redefs [environ.core/env mock-env
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 2 2))
                  oph.heratepalvelu.db.dynamodb/query-items mdb/query-items
                  oph.heratepalvelu.db.dynamodb/update-item mdb/update-item
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.koski/pwd (delay "koski-pwd")
                  oph.heratepalvelu.external.cas-client/cas-authenticated-post
                  mcc/mock-cas-authenticated-post
                  oph.heratepalvelu.external.cas-client/get-service-ticket
                  mcc/mock-get-service-ticket
                  oph.heratepalvelu.external.http-client/get mhc/mock-get
                  oph.heratepalvelu.external.http-client/post mhc/mock-post
                  oph.heratepalvelu.external.http-client/patch mhc/mock-patch]
      (setup-test)
      (heh/-handleSendAMISEmails {}
                                 (tu/mock-handler-event :scheduledherate)
                                 (tu/mock-handler-context))
      (let [actual-table-contents
            (mdb/get-table-values (:herate-table mock-env))]
        (is (= actual-table-contents expected-table-contents)
            (->> (diff actual-table-contents expected-table-contents)
                 (clojure.string/join "\n")
                 (str "differing items:\n"))))
      (is (= (mhc/get-results) expected-http-results)
          (->> (diff (mhc/get-results) expected-http-results)
               (clojure.string/join "\n")
               (str "differing items:\n")))
      (is (= (mcc/get-results) expected-cas-client-results)
          (->> (diff (mcc/get-results) expected-cas-client-results)
               (clojure.string/join "\n")
               (str "differing items:\n")))
      (teardown-test))))
