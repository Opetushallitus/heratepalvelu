(ns oph.heratepalvelu.integration-tests.amis.AMISMuistutusHandler-i-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.amis.AMISMuistutusHandler :as mh]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.integration-tests.mock-cas-client :as mcc]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.integration-tests.mock-http-client :as mhc]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(def mock-env {:herate-table "herate-table-name"
               :arvo-url "https://oph-arvo.com/"
               :arvo-user "arvo-user"
               :viestintapalvelu-url "https://oph-viestintapalvelu.com"})

(def starting-table-contents [{:toimija_oppija [:s "abc/123"]
                               :tyyppi_kausi [:s "aloittaneet/2021-2022"]
                               :muistutukset [:n 0]
                               :kyselylinkki [:s "kysely.linkki/123"]
                               :kyselytyyppi [:s "aloittaneet"]
                               :sahkoposti [:s "sahko.posti@esimerkki.fi"]
                               :lahetystila [:s (:success c/kasittelytilat)]
                               :lahetyspvm [:s "2022-01-25"]
                               :suorituskieli [:s "fi"]
                               :viestintapalvelu-id [:n 123]}
                              {:toimija_oppija [:s "lkj/245"]
                               :tyyppi_kausi [:s "aloittaneet/2022-2023"]
                               :muistutukset [:n 1]
                               :kyselylinkki [:s "kysely.linkki/245"]
                               :kyselytyyppi [:s "aloittaneet"]
                               :sahkoposti [:s "asdf@esimerkki.fi"]
                               :lahetystila [:s (:success c/kasittelytilat)]
                               :lahetyspvm [:s "2022-01-20"]
                               :suorituskieli [:s "fi"]
                               :viestintapalvelu-id [:n 245]}
                              {:toimija_oppija [:s "abc/333"]
                               :tyyppi_kausi [:s "aloittaneet/2021-2022"]
                               :muistutukset [:n 0]
                               :kyselylinkki [:s "kysely.linkki/333"]
                               :kyselytyyppi [:s "aloittaneet"]
                               :sahkoposti [:s "joku@esimerkki.fi"]
                               :lahetystila [:s (:success c/kasittelytilat)]
                               :lahetyspvm [:s "2022-01-25"]
                               :suorituskieli [:s "fi"]
                               :viestintapalvelu-id [:n 333]}
                              {:toimija_oppija [:s "lkj/444"]
                               :tyyppi_kausi [:s "paattyneet/2022-2023"]
                               :muistutukset [:n 1]
                               :kyselylinkki [:s "kysely.linkki/444"]
                               :kyselytyyppi [:s "tutkinnon_suorittaneet"]
                               :sahkoposti [:s "aaa@esimerkki.fi"]
                               :lahetystila [:s (:success c/kasittelytilat)]
                               :suorituskieli [:s "fi"]
                               :lahetyspvm [:s "2022-01-20"]
                               :viestintapalvelu-id [:n 444]}])

(defn- setup-test []
  (mcc/clear-results)
  (mcc/bind-url :post
                (:viestintapalvelu-url mock-env)
                {:recipient [{:email "sahko.posti@esimerkki.fi"}]
                 :email {:callingProcess "heratepalvelu"
                         :from "no-reply@opintopolku.fi"
                         :sender "Opetushallitus – Utbildningsstyrelsen – EDUFI"
                         :subject (str "Muistutus-påminnelse-reminder: "
                                       "Vastaa kyselyyn - svara på enkäten - "
                                       "answer the survey")
                         :isHtml true
                         :body (vp/amismuistutus-html
                                 {:suorituskieli "fi"
                                  :kyselylinkki "kysely.linkki/123"
                                  :kyselytyyppi "aloittaneet"})}}
                {:body {:id 111}})
  (mcc/bind-url :post
                (:viestintapalvelu-url mock-env)
                {:recipient [{:email "asdf@esimerkki.fi"}]
                 :email {:callingProcess "heratepalvelu"
                         :from "no-reply@opintopolku.fi"
                         :sender "Opetushallitus – Utbildningsstyrelsen – EDUFI"
                         :subject (str "Muistutus-påminnelse-reminder: "
                                       "Vastaa kyselyyn - svara på enkäten - "
                                       "answer the survey")
                         :isHtml true
                         :body (vp/amismuistutus-html
                                 {:suorituskieli "fi"
                                  :kyselylinkki "kysely.linkki/245"
                                  :kyselytyyppi "aloittaneet"})}}
                {:body {:id 222}})
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mhc/bind-url :get
                (str (:arvo-url mock-env) "vastauslinkki/v1/status/123")
                {:basic-auth [(:arvo-user mock-env) "arvo-pwd"] :as :json}
                {:body {:vastattu false :voimassa_loppupvm "2022-03-01"}})
  (mhc/bind-url :get
                (str (:arvo-url mock-env) "vastauslinkki/v1/status/245")
                {:basic-auth [(:arvo-user mock-env) "arvo-pwd"] :as :json}
                {:body {:vastattu false :voimassa_loppupvm "2022-03-03"}})
  (mhc/bind-url :get
                (str (:arvo-url mock-env) "vastauslinkki/v1/status/333")
                {:basic-auth [(:arvo-user mock-env) "arvo-pwd"] :as :json}
                {:body {:vastattu false :voimassa_loppupvm "2022-01-05"}})
  (mhc/bind-url :get
                (str (:arvo-url mock-env) "vastauslinkki/v1/status/444")
                {:basic-auth [(:arvo-user mock-env) "arvo-pwd"] :as :json}
                {:body {:vastattu true}})

  (mdb/clear-mock-db)
  (mdb/create-table (:herate-table mock-env) {:primary-key :toimija_oppija
                                              :sort-key :tyyppi_kausi})
  (mdb/set-table-contents (:herate-table mock-env) starting-table-contents))

(defn- teardown-test []
  (mcc/clear-results)
  (mcc/clear-url-bindings)
  (mhc/clear-results)
  (mhc/clear-url-bindings)
  (mdb/clear-mock-db))

(def expected-table-contents #{{:toimija_oppija [:s "abc/123"]
                                :tyyppi_kausi [:s "aloittaneet/2021-2022"]
                                :muistutukset [:n 1]
                                :kyselylinkki [:s "kysely.linkki/123"]
                                :kyselytyyppi [:s "aloittaneet"]
                                :sahkoposti [:s "sahko.posti@esimerkki.fi"]
                                :lahetystila [:s (:viestintapalvelussa
                                                   c/kasittelytilat)]
                                :lahetyspvm [:s "2022-01-25"]
                                :suorituskieli [:s "fi"]
                                :viestintapalvelu-id [:n 111]
                                :1.-muistutus-lahetetty [:s "2022-02-02"]}
                               {:toimija_oppija [:s "lkj/245"]
                                :tyyppi_kausi [:s "aloittaneet/2022-2023"]
                                :muistutukset [:n 2]
                                :kyselylinkki [:s "kysely.linkki/245"]
                                :kyselytyyppi [:s "aloittaneet"]
                                :sahkoposti [:s "asdf@esimerkki.fi"]
                                :lahetystila [:s (:viestintapalvelussa
                                                   c/kasittelytilat)]
                                :lahetyspvm [:s "2022-01-20"]
                                :suorituskieli [:s "fi"]
                                :viestintapalvelu-id [:n 222]
                                :2.-muistutus-lahetetty [:s "2022-02-02"]}
                               {:toimija_oppija [:s "abc/333"]
                                :tyyppi_kausi [:s "aloittaneet/2021-2022"]
                                :muistutukset [:n 1]
                                :kyselylinkki [:s "kysely.linkki/333"]
                                :kyselytyyppi [:s "aloittaneet"]
                                :sahkoposti [:s "joku@esimerkki.fi"]
                                :lahetystila [:s (:vastausaika-loppunut-m
                                                   c/kasittelytilat)]
                                :lahetyspvm [:s "2022-01-25"]
                                :suorituskieli [:s "fi"]
                                :viestintapalvelu-id [:n 333]}
                               {:toimija_oppija [:s "lkj/444"]
                                :tyyppi_kausi [:s "paattyneet/2022-2023"]
                                :muistutukset [:n 2]
                                :kyselylinkki [:s "kysely.linkki/444"]
                                :kyselytyyppi [:s "tutkinnon_suorittaneet"]
                                :sahkoposti [:s "aaa@esimerkki.fi"]
                                :lahetystila [:s (:vastattu c/kasittelytilat)]
                                :suorituskieli [:s "fi"]
                                :lahetyspvm [:s "2022-01-20"]
                                :viestintapalvelu-id [:n 444]}})

(def expected-cas-client-results
  [{:method :post
    :url "https://oph-viestintapalvelu.com"
    :body {:recipient [{:email "sahko.posti@esimerkki.fi"}]
           :email {:callingProcess "heratepalvelu"
                   :from "no-reply@opintopolku.fi"
                   :sender "Opetushallitus – Utbildningsstyrelsen – EDUFI"
                   :subject (str "Muistutus-påminnelse-reminder: "
                                 "Vastaa kyselyyn - svara på enkäten - "
                                 "answer the survey")
                   :isHtml true
                   :body (vp/amismuistutus-html
                           {:suorituskieli "fi"
                            :kyselylinkki "kysely.linkki/123"
                            :kyselytyyppi "aloittaneet"})}}
    :options {:as :json}}
   {:method :post
    :url "https://oph-viestintapalvelu.com"
    :body {:recipient [{:email "asdf@esimerkki.fi"}]
           :email {:callingProcess "heratepalvelu"
                   :from "no-reply@opintopolku.fi"
                   :sender "Opetushallitus – Utbildningsstyrelsen – EDUFI"
                   :subject (str "Muistutus-påminnelse-reminder: "
                                 "Vastaa kyselyyn - svara på enkäten - "
                                 "answer the survey")
                   :isHtml true
                   :body (vp/amismuistutus-html
                           {:suorituskieli "fi"
                            :kyselylinkki "kysely.linkki/245"
                            :kyselytyyppi "aloittaneet"})}}
    :options {:as :json}}])

(def expected-http-results
  [{:method :get
    :url "https://oph-arvo.com/vastauslinkki/v1/status/123"
    :options {:basic-auth ["arvo-user" "arvo-pwd"]
              :as :json}}
   {:method :get
    :url "https://oph-arvo.com/vastauslinkki/v1/status/333"
    :options {:basic-auth ["arvo-user" "arvo-pwd"]
              :as :json}}
   {:method :get
    :url "https://oph-arvo.com/vastauslinkki/v1/status/245"
    :options {:basic-auth ["arvo-user" "arvo-pwd"]
              :as :json}}
   {:method :get
    :url "https://oph-arvo.com/vastauslinkki/v1/status/444"
    :options {:basic-auth ["arvo-user" "arvo-pwd"]
              :as :json}}])

(deftest test-AMISMuistutusHandler-integration
  (testing "AMISMuistutusHandler integraatiotesti"
    (with-redefs [environ.core/env mock-env
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 2 2))
                  oph.heratepalvelu.db.dynamodb/query-items mdb/query-items
                  oph.heratepalvelu.db.dynamodb/update-item mdb/update-item
                  oph.heratepalvelu.external.arvo/pwd (delay "arvo-pwd")
                  oph.heratepalvelu.external.cas-client/cas-authenticated-post
                  mcc/mock-cas-authenticated-post
                  oph.heratepalvelu.external.cas-client/get-service-ticket
                  mcc/mock-get-service-ticket
                  oph.heratepalvelu.external.http-client/get mhc/mock-get]
      (setup-test)
      (mh/-handleSendAMISMuistutus {}
                                   (tu/mock-handler-event :scheduledherate)
                                   (tu/mock-handler-context))
      (is (= (mdb/get-table-values (:herate-table mock-env))
             expected-table-contents))
      (is (= (mcc/get-results) expected-cas-client-results))
      (is (= (mhc/get-results) expected-http-results))
      (teardown-test))))
