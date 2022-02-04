(ns oph.heratepalvelu.integration-tests.amis.AMISDeleteTunnusHandler-i-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.amis.AMISDeleteTunnusHandler :as dth]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.test-util :as tu]))

(def mock-env {:herate-table "herate-table-name"})

(def starting-table-contents [{:toimija_oppija [:s "abc/123"]
                               :tyyppi_kausi [:s "aloittaneet/2021-2022"]
                               :muistutukset [:n 2]
                               :kyselylinkki [:s "kysely.linkki/123"]
                               :sahkoposti [:s "sahko.posti@esimerkki.fi"]
                               :lahetystila [:s (:viestintapalvelussa
                                                  c/kasittelytilat)]
                               :viestintapalvelu-id [:n 123]}
                              {:toimija_oppija [:s "lkj/245"]
                               :tyyppi_kausi [:s "paattyneet/2022-2023"]
                               :kyselylinkki [:s "kysely.linkki/245"]
                               :sahkoposti [:s "asdf@esimerkki.fi"]
                               :lahetystila [:s (:viestintapalvelussa
                                                  c/kasittelytilat)]
                               :viestintapalvelu-id [:n 245]}])

(defn- setup-test []
  (mdb/clear-mock-db)
  (mdb/create-table (:herate-table mock-env) {:primary-key :toimija_oppija
                                              :sort-key :tyyppi_kausi})
  (mdb/set-table-contents (:herate-table mock-env) starting-table-contents))

(def expected-table-contents #{{:toimija_oppija [:s "lkj/245"]
                                :tyyppi_kausi [:s "paattyneet/2022-2023"]
                                :kyselylinkki [:s "kysely.linkki/245"]
                                :sahkoposti [:s "asdf@esimerkki.fi"]
                                :lahetystila [:s (:viestintapalvelussa
                                                   c/kasittelytilat)]
                                :viestintapalvelu-id [:n 245]}})

(deftest test-AMISDeleteTunnusHandler-integration
  (testing "AMISDeleteTunnusHandler integraatiotesti"
    (with-redefs [environ.core/env mock-env
                  oph.heratepalvelu.db.dynamodb/delete-item mdb/delete-item
                  oph.heratepalvelu.db.dynamodb/query-items mdb/query-items]
      (setup-test)
      (dth/-handleDeleteTunnus {}
                               (tu/mock-sqs-event
                                 {:kyselylinkki "kysely.linkki/123"})
                               (tu/mock-handler-context))
      (is (= (mdb/get-table-values (:herate-table mock-env))
          expected-table-contents))
      (mdb/clear-mock-db))))
