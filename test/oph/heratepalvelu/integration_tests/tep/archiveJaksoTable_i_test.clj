(ns oph.heratepalvelu.integration-tests.tep.archiveJaksoTable-i-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.tep.archiveJaksoTable :as ajt]
            [oph.heratepalvelu.test-util :as tu]))

(def mock-env {:jaksotunnus-table       "jaksotunnus-table-name"
               :archive-table-2021-2022 "archive_2021-2022"
               :archive-table-2022-2023 "archive_2022-2023"})

(def starting-table [{:hankkimistapa_id [:n 1]
                      :rahoituskausi    [:s "2021-2022"]}
                     {:hankkimistapa_id [:n 2]
                      :rahoituskausi    [:s "2022-2023"]}
                     {:hankkimistapa_id [:n 3]
                      :rahoituskausi    [:s "2023-2024"]}])

(defn- setup-test []
  (mdb/clear-mock-db)
  (mdb/create-table (:jaksotunnus-table mock-env)
                    {:primary-key :hankkimistapa_id})
  (mdb/set-table-contents (:jaksotunnus-table mock-env) starting-table)
  (mdb/create-table (:archive-table-2021-2022 mock-env)
                    {:primary-key :hankkimistapa_id})
  (mdb/set-table-contents (:archive-table-2021-2022 mock-env) []))

(def expected-table #{{:hankkimistapa_id [:n 3]
                       :rahoituskausi    [:s "2023-2024"]}})

(def expected-archive-2021-2022 #{{:hankkimistapa_id [:n 1]
                                   :rahoituskausi    [:s "2021-2022"]}})

(def expected-archive-2022-2023 #{{:hankkimistapa_id [:n 2]
                                   :rahoituskausi    [:s "2022-2023"]}})

(deftest test-archiveJaksoTable-integration
  (testing "archiveJaksoTable integraatiotesti"
    (with-redefs [environ.core/env                          mock-env
                  oph.heratepalvelu.db.dynamodb/delete-item mdb/delete-item
                  oph.heratepalvelu.db.dynamodb/put-item    mdb/put-item
                  oph.heratepalvelu.db.dynamodb/scan        mdb/scan]
      (setup-test)
      (ajt/-archiveJaksoTable {}
                              (tu/mock-handler-event :scheduledherate)
                              (tu/mock-handler-context))
      (is (= (mdb/get-table-values (:jaksotunnus-table mock-env))
             expected-table))
      (is (= (mdb/get-table-values (:archive-table-2021-2022 mock-env))
             expected-archive-2021-2022))
      (is (= (mdb/get-table-values (:archive-table-2022-2023 mock-env))
             expected-archive-2022-2023))
      (mdb/clear-mock-db))))
