(ns oph.heratepalvelu.integration-tests.tpk.archiveTpkNippuTable-i-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.test-util :as tu]
            [oph.heratepalvelu.tpk.archiveTpkNippuTable :as atnt]))

(def mock-env {:tpk-nippu-table           "tpk-nippu-table-name"
               :archive-table-2021-fall   "archive_2021-fall"
               :archive-table-2022-spring "archive_2022-spring"
               :archive-table-2022-fall   "archive_2022-fall"
               :archive-table-2023-spring "archive_2023-spring"})

(def starting-table [{:nippu-id            [:s "test-nippu-id"]
                      :tiedonkeruu-alkupvm [:s "2021-07-01"]}
                     {:nippu-id            [:s "test-nippu-id-2"]
                      :tiedonkeruu-alkupvm [:s "2022-01-01"]}
                     {:nippu-id            [:s "test-nippu-id-3"]
                      :tiedonkeruu-alkupvm [:s "2022-07-01"]}
                     {:nippu-id            [:s "test-nippu-id-4"]
                      :tiedonkeruu-alkupvm [:s "2023-01-01"]}
                     {:nippu-id            [:s "test-nippu-id-5"]
                      :tiedonkeruu-alkupvm [:s "2023-07-01"]}])

(defn- setup-test []
  (mdb/clear-mock-db)
  (mdb/create-table (:tpk-nippu-table mock-env) {:primary-key :nippu-id})
  (mdb/set-table-contents (:tpk-nippu-table mock-env) starting-table)
  (mdb/create-table (:archive-table-2021-fall mock-env)
                    {:primary-key :nippu-id})
  (mdb/set-table-contents (:archive-table-2021-fall mock-env) []))

(def expected-table #{{:nippu-id            [:s "test-nippu-id-5"]
                       :tiedonkeruu-alkupvm [:s "2023-07-01"]}})

(def expected-archive-2021-fall   #{{:nippu-id            [:s "test-nippu-id"]
                                     :tiedonkeruu-alkupvm [:s "2021-07-01"]}})

(def expected-archive-2022-spring #{{:nippu-id            [:s "test-nippu-id-2"]
                                     :tiedonkeruu-alkupvm [:s "2022-01-01"]}})

(def expected-archive-2022-fall   #{{:nippu-id            [:s "test-nippu-id-3"]
                                     :tiedonkeruu-alkupvm [:s "2022-07-01"]}})

(def expected-archive-2023-spring #{{:nippu-id            [:s "test-nippu-id-4"]
                                     :tiedonkeruu-alkupvm [:s "2023-01-01"]}})

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
      (is (= (mdb/get-table-values (:archive-table-2022-spring mock-env))
             expected-archive-2022-spring))
      (is (= (mdb/get-table-values (:archive-table-2022-fall mock-env))
             expected-archive-2022-fall))
      (is (= (mdb/get-table-values (:archive-table-2023-spring mock-env))
             expected-archive-2023-spring))
      (mdb/clear-mock-db))))
