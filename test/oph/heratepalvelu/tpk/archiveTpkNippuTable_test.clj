(ns oph.heratepalvelu.tpk.archiveTpkNippuTable-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.tpk.archiveTpkNippuTable :as atnt]
            [oph.heratepalvelu.test-util :as tu]))

(defn- mock-scan [options table] {:options options :table table})

(deftest test-do-query
  (testing "Varmista, että do-query toimii odotuksenmukaisesti"
    (with-redefs [environ.core/env {:tpk-nippu-table "tpk-nippu-table-name"}
                  oph.heratepalvelu.db.dynamodb/scan mock-scan]
      (is (= (atnt/do-query "2021-07-01" nil)
             {:options {:filter-expression "#kausi = :kausi"
                        :expr-attr-names   {"#kausi" "tiedonkeruu-alkupvm"}
                        :expr-attr-vals    {":kausi" [:s "2021-07-01"]}}
              :table   "tpk-nippu-table-name"}))
      (is (= (atnt/do-query "2022-01-01" "asdf")
             {:options {:filter-expression   "#kausi = :kausi"
                        :expr-attr-names     {"#kausi" "tiedonkeruu-alkupvm"}
                        :expr-attr-vals      {":kausi" [:s "2022-01-01"]}
                        :exclusive-start-key "asdf"}
              :table   "tpk-nippu-table-name"})))))

(def results (atom []))

(defn- mock-do-query [kausi-alkupvm last-evaluated-key]
  (reset! results (cons {:type "mock-do-query"
                         :kausi-alkupvm kausi-alkupvm
                         :last-evaluated-key last-evaluated-key}
                        @results))
  {:items [{:nippu-id "test-nippu-id" :tiedonkeruu-alkupvm "2021-07-01"}]})

(defn- mock-put-item [item options table]
  (reset! results (cons {:type "mock-put-item"
                         :item item
                         :options options
                         :table table}
                        @results)))

(defn- mock-delete-item [key-conds table]
  (reset! results (cons {:type "mock-delete-item"
                         :key-conds key-conds
                         :table table}
                        @results)))

(deftest test-do-tpk-nippu-table-archiving
  (testing "Varmista, että do-tpk-nippu-table-archiving toimii oikein"
    (with-redefs [environ.core/env {:tpk-nippu-table "tpk-nippu-table-name"}
                  oph.heratepalvelu.db.dynamodb/delete-item mock-delete-item
                  oph.heratepalvelu.db.dynamodb/put-item mock-put-item
                  oph.heratepalvelu.tpk.archiveTpkNippuTable/do-query
                  mock-do-query]
      (atnt/do-tpk-nippu-table-archiving "2021-07-01" "to-table-name")
      (is (= (vec (reverse @results))
             [{:type               "mock-do-query"
               :kausi-alkupvm      "2021-07-01"
               :last-evaluated-key nil}
              {:type    "mock-put-item"
               :item    {:nippu-id            [:s "test-nippu-id"]
                         :tiedonkeruu-alkupvm [:s "2021-07-01"]}
               :options {}
               :table   "to-table-name"}
              {:type      "mock-delete-item"
               :key-conds {:nippu-id            [:s "test-nippu-id"]}
               :table     "tpk-nippu-table-name"}])))))

(def test-archiveTpkNippuTable-results (atom []))

(defn- mock-do-tpk-nippu-table-archiving [kausi-alkupvm to-table]
  (reset! test-archiveTpkNippuTable-results
          (cons {:kausi-alkupvm kausi-alkupvm :to-table to-table}
                @test-archiveTpkNippuTable-results)))

(deftest test-archiveTpkNippuTable
  (testing "Varmista, että -archiveTpkNippuTable tekee oikeita kutsuja"
    (with-redefs
      [environ.core/env {:archive-table-2021-fall   "archive_2021-fall"
                         :archive-table-2022-spring "archive_2022-spring"
                         :archive-table-2022-fall   "archive_2022-fall"
                         :archive-table-2023-spring "archive_2023-spring"}
       oph.heratepalvelu.tpk.archiveTpkNippuTable/do-tpk-nippu-table-archiving
       mock-do-tpk-nippu-table-archiving]
      (atnt/-archiveTpkNippuTable {}
                                  (tu/mock-handler-event :scheduledherate)
                                  (tu/mock-handler-context))
      (is (= (vec (reverse @test-archiveTpkNippuTable-results))
             [{:kausi-alkupvm "2021-07-01" :to-table "archive_2021-fall"}
              {:kausi-alkupvm "2022-01-01" :to-table "archive_2022-spring"}
              {:kausi-alkupvm "2022-07-01" :to-table "archive_2022-fall"}
              {:kausi-alkupvm "2023-01-01"
               :to-table "archive_2023-spring"}])))))
