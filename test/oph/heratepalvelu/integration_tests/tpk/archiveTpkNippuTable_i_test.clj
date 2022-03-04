(ns oph.heratepalvelu.integration-tests.tpk.archiveTpkNippuTable-i-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.test-util :as tu]
            [oph.heratepalvelu.tpk.archiveTpkNippuTable :as atnt]))

(def mock-env {:tpk-nippu-table         "tpk-nippu-table-name"
               :archive-table-2021-fall "archive_2021-fall"})

(def starting-table [{:nippu-id            [:s "test-nippu-id"]
                      :tiedonkeruu-alkupvm [:s "2021-07-01"]}
                     {:nippu-id            [:s "test-nippu-id-2"]
                      :tiedonkeruu-alkupvm [:s "2022-01-01"]}])

(defn- setup-test []
  (mdb/clear-mock-db)
  (mdb/create-table (:tpk-nippu-table mock-env)
                    {:primary-key :nippu-id
                     :sort-key    :tiedonkeruu-alkupvm})
  (mdb/set-table-contents (:tpk-nippu-table mock-env) starting-table)
  (mdb/create-table (:archive-table-2021-fall mock-env)
                    {:primary-key :nippu-id})
  (mdb/set-table-contents (:archive-table-2021-fall mock-env) []))

(def expected-table #{{:nippu-id            [:s "test-nippu-id-2"]
                       :tiedonkeruu-alkupvm [:s "2022-01-01"]}})

(def expected-archive-2021-fall #{{:nippu-id            [:s "test-nippu-id"]
                                   :tiedonkeruu-alkupvm [:s "2021-07-01"]}})

(deftest test-archiveTpkNippuTable-integration
  (testing "archiveTpkNippuTable integraatiotesti"
    (with-redefs [environ.core/env                          mock-env
                  oph.heratepalvelu.db.dynamodb/delete-item mdb/delete-item
                  oph.heratepalvelu.db.dynamodb/put-item    mdb/put-item
                  oph.heratepalvelu.db.dynamodb/scan        mdb/scan]
      (setup-test)
      (atnt/-archiveTpkNippuTable {}
                                  (tu/mock-handler-event :scheduledherate)
                                  (tu/mock-handler-context))
      (is (= (mdb/get-table-values (:tpk-nippu-table mock-env)) expected-table))
      (is (= (mdb/get-table-values (:archive-table-2021-fall mock-env))
             expected-archive-2021-fall))
      (mdb/clear-mock-db))))
