(ns oph.heratepalvelu.tep.archiveJaksoTable-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.tep.archiveJaksoTable :as ajt]
            [oph.heratepalvelu.test-util :as tu]))

(defn- mock-scan [options table] {:options options :table table})

(deftest test-do-query
  (testing "Varmista, että do-query kutsuu scania oikeilla parametreilla."
    (with-redefs [environ.core/env {:jaksotunnus-table "jaksotunnus-table-name"}
                  oph.heratepalvelu.db.dynamodb/scan mock-scan]
      (is (= (ajt/do-query "2021-2022" nil)
             {:options {:filter-expression "rahoituskausi = :kausi"
                        :expr-attr-vals    {":kausi" [:s "2021-2022"]}}
              :table   "jaksotunnus-table-name"}))
      (is (= (ajt/do-query "2022-2023" "asdf")
             {:options {:filter-expression   "rahoituskausi = :kausi"
                        :expr-attr-vals      {":kausi" [:s "2022-2023"]}
                        :exclusive-start-key "asdf"}
              :table   "jaksotunnus-table-name"})))))


(def results (atom []))

(defn- mock-do-query [kausi last-evaluated-key]
  (reset! results (cons {:type "mock-do-query"
                         :kausi kausi
                         :last-evaluated-key last-evaluated-key}
                        @results))
  {:items [{:hankkimistapa_id 123}]})

(defn- mock-put-item [key-conds options table]
  (reset! results
          (cons {:type "mock-put-item"
                 :key-conds key-conds
                 :options options
                 :table table}
                @results)))

(defn- mock-delete-item [key-conds table]
  (reset! results (cons {:type "mock-delete-item"
                         :key-conds key-conds
                         :table table}
                        @results)))

(deftest test-do-jakso-table-archiving
  (testing "Varmista, että do-jakso-table-archiving tekee oikeita kutsuja"
    (with-redefs [environ.core/env {:jaksotunnus-table "jaksotunnus-table-name"}
                  oph.heratepalvelu.db.dynamodb/delete-item mock-delete-item
                  oph.heratepalvelu.db.dynamodb/put-item mock-put-item
                  oph.heratepalvelu.tep.archiveJaksoTable/do-query
                  mock-do-query]
      (ajt/do-jakso-table-archiving "2021-2022" "to-table-name")
      (is (= (vec (reverse @results))
             [{:type "mock-do-query" :kausi "2021-2022" :last-evaluated-key nil}
              {:type "mock-put-item"
               :key-conds {:hankkimistapa_id [:n 123]}
               :options {}
               :table "to-table-name"}
              {:type "mock-delete-item"
               :key-conds {:hankkimistapa_id [:n 123]}
               :table "jaksotunnus-table-name"}])))))


(def test-archiveJaksoTable-results (atom []))

(defn- mock-do-jakso-table-archiving [kausi to-table]
  (reset! test-archiveJaksoTable-results
          (cons {:kausi kausi :to-table to-table}
                @test-archiveJaksoTable-results)))

(deftest test-archiveJaksoTable
  (testing "Varmista, että -archiveJaksoTable tekee oikeita kutsuja."
    (with-redefs
      [environ.core/env {:archive-table-2021-2022 "archive_2021-2022"}
       oph.heratepalvelu.tep.archiveJaksoTable/do-jakso-table-archiving
       mock-do-jakso-table-archiving]
      (ajt/-archiveJaksoTable {}
                              (tu/mock-handler-event :scheduledherate)
                              (tu/mock-handler-context))
      (is (= (vec (reverse @test-archiveJaksoTable-results))
             [{:kausi "2021-2022" :to-table "archive_2021-2022"}])))))
