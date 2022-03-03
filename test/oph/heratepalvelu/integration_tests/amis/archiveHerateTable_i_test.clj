(ns oph.heratepalvelu.integration-tests.amis.archiveHerateTable-i-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.amis.archiveHerateTable :as aht]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.test-util :as tu]))

(def mock-env {:from-table         "herate-table-name"
               :to-table           "archive_2019-2020"
               :to-table-2020-2021 "archive_2020-2021"
               :to-table-2021-2022 "archive_2021-2022"})

(def starting-table [{:toimija_oppija [:s "toimija-oppija-1"]
                      :tyyppi_kausi   [:s "tyyppi-kausi-1"]
                      :rahoituskausi  [:s "2019-2020"]}
                     {:toimija_oppija [:s "toimija-oppija-2"]
                      :tyyppi_kausi   [:s "tyyppi-kausi-2"]
                      :rahoituskausi  [:s "2020-2021"]}
                     {:toimija_oppija [:s "toimija-oppija-3"]
                      :tyyppi_kausi   [:s "tyyppi-kausi-3"]
                      :rahoituskausi  [:s "2021-2022"]}
                     {:toimija_oppija [:s "toimija-oppija-4"]
                      :tyyppi_kausi   [:s "tyyppi-kausi-4"]
                      :rahoituskausi  [:s "2022-2023"]}])

(defn- setup-test []
  (mdb/clear-mock-db)
  (mdb/create-table (:from-table mock-env) {:primary-key :toimija_oppija
                                            :sort-key    :tyyppi_kausi})
  (mdb/set-table-contents (:from-table mock-env) starting-table)
  (mdb/create-table (:to-table mock-env) {:primary-key :toimija_oppija
                                          :sort-key    :tyyppi_kausi})
  (mdb/set-table-contents (:to-table mock-env) [])
  (mdb/create-table (:to-table-2020-2021 mock-env) {:primary-key :toimija_oppija
                                                    :sort-key    :tyyppi_kausi})
  (mdb/set-table-contents (:to-table-2020-2021 mock-env) [])
  (mdb/create-table (:to-table-2021-2022 mock-env) {:primary-key :toimija_oppija
                                                    :sort-key    :tyyppi_kausi})
  (mdb/set-table-contents (:to-table-2021-2022 mock-env) []))

(def expected-table #{{:toimija_oppija [:s "toimija-oppija-4"]
                       :tyyppi_kausi   [:s "tyyppi-kausi-4"]
                       :rahoituskausi  [:s "2022-2023"]}})

(def expected-to-table #{{:toimija_oppija [:s "toimija-oppija-1"]
                          :tyyppi_kausi   [:s "tyyppi-kausi-1"]
                          :rahoituskausi  [:s "2019-2020"]}})

(def expected-2020-2021-table #{{:toimija_oppija [:s "toimija-oppija-2"]
                                 :tyyppi_kausi   [:s "tyyppi-kausi-2"]
                                 :rahoituskausi  [:s "2020-2021"]}})

(def expected-2021-2022-table #{{:toimija_oppija [:s "toimija-oppija-3"]
                                 :tyyppi_kausi   [:s "tyyppi-kausi-3"]
                                 :rahoituskausi  [:s "2021-2022"]}})

(deftest test-archiveHerateTable-integration
  (testing "archiveHerateTable integraatiotesti"
    (with-redefs [environ.core/env                          mock-env
                  oph.heratepalvelu.db.dynamodb/delete-item mdb/delete-item
                  oph.heratepalvelu.db.dynamodb/put-item    mdb/put-item
                  oph.heratepalvelu.db.dynamodb/scan        mdb/scan]
      (setup-test)
      (aht/-archiveHerateTable {}
                               (tu/mock-handler-event :scheduledherate)
                               (tu/mock-handler-context))
      (is (= (mdb/get-table-values (:to-table mock-env)) expected-to-table))
      (is (= (mdb/get-table-values (:to-table-2020-2021 mock-env))
             expected-2020-2021-table))
      (is (= (mdb/get-table-values (:to-table-2021-2022 mock-env))
             expected-2021-2022-table))
      (mdb/clear-mock-db))))
