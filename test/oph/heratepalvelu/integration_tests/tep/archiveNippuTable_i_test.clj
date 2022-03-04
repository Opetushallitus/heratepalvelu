(ns oph.heratepalvelu.integration-tests.tep.archiveNippuTable-i-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.tep.archiveNippuTable :as ant]
            [oph.heratepalvelu.test-util :as tu]))

(def mock-env {:nippu-table             "nippu-table-name"
               :archive-table-2021-2022 "archive_2021-2022"})

(def starting-table [{:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
                      :niputuspvm                  [:s "2021-07-16"]}
                     {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-2"]
                      :niputuspvm                  [:s "2022-07-01"]}
                     {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-3"]
                      :niputuspvm                  [:s "2022-08-16"]}])

(defn- setup-test []
  (mdb/clear-mock-db)
  (mdb/create-table (:nippu-table mock-env)
                    {:primary-key :ohjaaja_ytunnus_kj_tutkinto
                     :sort-key    :niputuspvm})
  (mdb/set-table-contents (:nippu-table mock-env) starting-table)
  (mdb/create-table (:archive-table-2021-2022 mock-env)
                    {:primary-key :ohjaaja_ytunnus_kj_tutkinto
                     :sort-key    :niputuspvm})
  (mdb/set-table-contents (:archive-table-2021-2022 mock-env) []))

(def expected-table #{{:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-3"]
                       :niputuspvm                  [:s "2022-08-16"]}})

(def expected-archive-2021-2022
  #{{:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-1"]
     :niputuspvm                  [:s "2021-07-16"]}
    {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt-2"]
     :niputuspvm                  [:s "2022-07-01"]}})

(deftest test-archiveNippuTable-integration
  (testing "archiveNippuTable integraatiotesti"
    (with-redefs [environ.core/env                          mock-env
                  oph.heratepalvelu.db.dynamodb/delete-item mdb/delete-item
                  oph.heratepalvelu.db.dynamodb/put-item    mdb/put-item
                  oph.heratepalvelu.db.dynamodb/scan        mdb/scan]
      (setup-test)
      (ant/-archiveNippuTable {}
                              (tu/mock-handler-event :scheduledherate)
                              (tu/mock-handler-context))
      (is (= (mdb/get-table-values (:nippu-table mock-env)) expected-table))
      (is (= (mdb/get-table-values (:archive-table-2021-2022 mock-env))
             expected-archive-2021-2022))
      (mdb/clear-mock-db))))
