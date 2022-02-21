(ns oph.heratepalvelu.integration-tests.util.dbArchiver-i-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.test-util :as tu]
            [oph.heratepalvelu.util.dbArchiver :as dba]))

(def mock-env {:from-table "from-table-name"
               :to-table "to-table-name"})

(def starting-from-table [{:toimija_oppija [:s "t-o-1"]
                           :tyyppi_kausi [:s "t-k-1"]
                           :rahoituskausi [:s "2021-2022"]}
                          {:toimija_oppija [:s "t-o-2"]
                           :tyyppi_kausi [:s "t-k-2"]
                           :rahoituskausi [:s "2020-2021"]}
                          {:toimija_oppija [:s "t-o-3"]
                           :tyyppi_kausi [:s "t-k-3"]
                           :rahoituskausi [:s "2021-2022"]}
                          {:toimija_oppija [:s "t-o-4"]
                           :tyyppi_kausi [:s "t-k-4"]
                           :rahoituskausi [:s "2020-2021"]}
                          {:toimija_oppija [:s "t-o-5"]
                           :tyyppi_kausi [:s "t-k-5"]
                           :rahoituskausi [:s "2019-2020"]}
                          {:toimija_oppija [:s "t-o-6"]
                           :tyyppi_kausi [:s "t-k-6"]
                           :rahoituskausi [:s "2019-2020"]}
                          {:toimija_oppija [:s "t-o-7"]
                           :tyyppi_kausi [:s "t-k-7"]
                           :rahoituskausi [:s "2020-2021"]}])

(defn setup-test []
  (mdb/clear-mock-db)
  (mdb/create-table (:from-table mock-env) {:primary-key :toimija_oppija
                                            :sort-key :tyyppi_kausi})
  (mdb/set-table-contents (:from-table mock-env) starting-from-table)
  (mdb/create-table (:to-table mock-env) {:primary-key :toimija_oppija
                                          :sory-key :tyyppi_kausi})
  (mdb/set-table-contents (:to-table mock-env) []))

(def expected-from-table #{{:toimija_oppija [:s "t-o-1"]
                            :tyyppi_kausi [:s "t-k-1"]
                            :rahoituskausi [:s "2021-2022"]}
                           {:toimija_oppija [:s "t-o-3"]
                            :tyyppi_kausi [:s "t-k-3"]
                            :rahoituskausi [:s "2021-2022"]}
                           {:toimija_oppija [:s "t-o-5"]
                            :tyyppi_kausi [:s "t-k-5"]
                            :rahoituskausi [:s "2019-2020"]}
                           {:toimija_oppija [:s "t-o-6"]
                            :tyyppi_kausi [:s "t-k-6"]
                            :rahoituskausi [:s "2019-2020"]}})

(def expected-to-table #{{:toimija_oppija [:s "t-o-2"]
                          :tyyppi_kausi [:s "t-k-2"]
                          :rahoituskausi [:s "2020-2021"]}
                         {:toimija_oppija [:s "t-o-4"]
                          :tyyppi_kausi [:s "t-k-4"]
                          :rahoituskausi [:s "2020-2021"]}
                         {:toimija_oppija [:s "t-o-7"]
                          :tyyppi_kausi [:s "t-k-7"]
                          :rahoituskausi [:s "2020-2021"]}})

(deftest test-doArchiving
  (testing "doArchiving integraatiotesti"
    (with-redefs [environ.core/env mock-env
                  oph.heratepalvelu.db.dynamodb/delete-item mdb/delete-item
                  oph.heratepalvelu.db.dynamodb/put-item mdb/put-item
                  oph.heratepalvelu.db.dynamodb/scan mdb/scan]
      (setup-test)
      (dba/doArchiving "2020-2021" (:to-table mock-env))
      (is (= (mdb/get-table-values (:from-table mock-env)) expected-from-table))
      (is (= (mdb/get-table-values (:to-table mock-env)) expected-to-table))
      (mdb/clear-mock-db))))
