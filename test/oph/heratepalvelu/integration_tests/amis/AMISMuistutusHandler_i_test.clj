(ns oph.heratepalvelu.integration-tests.amis.AMISMuistutusHandler-i-test
  (:require [clojure.test :refer :all]

            [oph.heratepalvelu.amis.AMISMuistutusHandler :as mh]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]


;; TODO

            [oph.heratepalvelu.test-util :as tu]))

(def mock-env {:herate-table "herate-table-name"

               ;; TODO


               })

(def starting-table-contents [

                              ;; TODO


                              ])

(defn- setup-test []
  ;; TODO clear other results?

  ;; TODO

  (mdb/clear-mock-db)
  (mdb/create-table (:herate-table mock-env) {:primary-key :toimija_oppija
                                              :sort-key :tyyppi_kausi})
  (mdb/set-table-contents (:herate-table mock-env) starting-table-contents))

(defn- teardown-test []
  ;; TODO other clears

  (mdb/clear-mock-db))

(def expected-table-contents #{

                               ;; TODO

                               })

;; TODO other expected results

(deftest test-AMISMuistutusHandler-integration
  (testing "AMISMuistutusHandler integraatiotesti"
    (with-redefs [environ.core/env mock-env


                  oph.heratepalvelu.db.dynamodb/query-items mdb/query-items
                  oph.heratepalvelu.db.dynamodb/update-item mdb/update-item

                  ;; TODO


                  ]
      (setup-test)
      (mh/-handleSendAMISMuistutus {}
                                   (tu/mock-handler-event :scheduledherate)
                                   (tu/mock-handler-contexts))
      (is (= (mdb/get-table-values (:herate-table mock-env))
             expected-table-contents))


      ;; TODO other checks


      (teardown-test))))
