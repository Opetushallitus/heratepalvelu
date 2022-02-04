(ns oph.heratepalvelu.integration-tests.amis.AMISEmailResendHandler-i-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.amis.AMISEmailResendHandler :as erh]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.test-util :as tu]))

(def mock-env {:herate-table "herate-table-name"})

(def starting-table-contents [{:toimija_oppija [:s "abc/123"]
                               :tyyppi_kausi [:s "aloittaneet/2021-2022"]
                               :muistutukset [:n 2]
                               :kyselylinkki [:s "kysely.linkki/123"]
                               :sahkoposti [:s "sahko.posti@esimerkki.fi"]
                               :lahetystila [:s (:success c/kasittelytilat)]
                               :viestintapalvelu-id [:n 123]}])

(defn- setup-test []
  (mdb/clear-mock-db)
  (mdb/create-table (:herate-table mock-env) {:primary-key :toimija_oppija
                                              :sort-key :tyyppi_kausi})
  (mdb/set-table-contents (:herate-table mock-env) starting-table-contents))

(def expected-table-contents #{{:toimija_oppija [:s "abc/123"]
                                :tyyppi_kausi [:s "aloittaneet/2021-2022"]
                                :muistutukset [:n 2]
                                :kyselylinkki [:s "kysely.linkki/123"]
                                :sahkoposti [:s "new@esimerkki.fi"]
                                :lahetystila [:s (:ei-lahetetty
                                                   c/kasittelytilat)]
                                :viestintapalvelu-id [:n 123]}})

(deftest test-AMISEmailResendHandler-integration
  (testing "AMISEmailResendHandlerin integraatiotesti"
    (with-redefs [environ.core/env mock-env
                  oph.heratepalvelu.db.dynamodb/query-items mdb/query-items
                  oph.heratepalvelu.db.dynamodb/update-item mdb/update-item]
      (setup-test)
      (erh/-handleEmailResend {}
                              (tu/mock-sqs-event
                                {:kyselylinkki "kysely.linkki/123"
                                 :sahkoposti "new@esimerkki.fi"})
                              (tu/mock-handler-context))
      (is (= (mdb/get-table-values (:herate-table mock-env))
             expected-table-contents))
      (mdb/clear-mock-db))))
