(ns oph.heratepalvelu.amis.archiveHerateTable-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.amis.archiveHerateTable :as aht]
            [oph.heratepalvelu.test-util :as tu]))

(def test-do-herate-table-archiving-results (atom []))

(defn- add-to-results [item]
  (reset! test-do-herate-table-archiving-results
          (cons item @test-do-herate-table-archiving-results)))

(defn- mock-scan [options table]
  (add-to-results {:type "mock-scan" :options options :table table})
  {:items [{:toimija_oppija "toimija-oppija"
            :tyyppi_kausi   "tyyppi-kausi"
            :kyselylinkki   "kysely.linkki/123"}]})

(defn- mock-put-item [item options table]
  (add-to-results {:type "mock-put-item"
                   :item item
                   :options options
                   :table table}))

(defn- mock-delete-item [key-conds table]
  (add-to-results {:type "mock-delete-item" :key-conds key-conds :table table}))

(deftest test-do-herate-table-archiving
  (testing "Varmista, että do-herate-table-archiving toimii odotuksenmukaisesti"
    (with-redefs [environ.core/env {:from-table "from-table-name"}
                  oph.heratepalvelu.db.dynamodb/delete-item mock-delete-item
                  oph.heratepalvelu.db.dynamodb/put-item    mock-put-item
                  oph.heratepalvelu.db.dynamodb/scan        mock-scan]
      (aht/do-herate-table-archiving "2019-2020" "to-table-name")
      (is (= (vec (reverse @test-do-herate-table-archiving-results))
             [{:type "mock-scan"
               :options {:filter-expression "rahoituskausi = :kausi"
                         :expr-attr-vals    {":kausi" [:s "2019-2020"]}}
               :table "from-table-name"}
              {:type "mock-put-item"
               :item {:toimija_oppija [:s "toimija-oppija"]
                      :tyyppi_kausi   [:s "tyyppi-kausi"]
                      :kyselylinkki   [:s "kysely.linkki/123"]}
               :options {}
               :table "to-table-name"}
              {:type "mock-delete-item"
               :key-conds {:toimija_oppija [:s "toimija-oppija"]
                           :tyyppi_kausi   [:s "tyyppi-kausi"]}
               :table "from-table-name"}])))))

(def test-archiveHerateTable-results (atom []))

(defn- mock-do-herate-table-archiving [kausi to-table]
  (reset! test-archiveHerateTable-results
          (cons {:kausi kausi :to-table to-table}
                @test-archiveHerateTable-results)))

(deftest test-archiveHerateTable
  (testing "Varmista, että -archiveHerateTable tekee oikeita kutsuja"
    (with-redefs
      [environ.core/env {:to-table           "archive_2019-2020"
                         :to-table-2020-2021 "archive_2020-2021"
                         :to-table-2021-2022 "archive_2021-2022"
                         :to-table-2022-2023 "archive_2022-2023"}
       oph.heratepalvelu.amis.archiveHerateTable/do-herate-table-archiving
       mock-do-herate-table-archiving]
      (aht/-archiveHerateTable {}
                               (tu/mock-handler-event :scheduledherate)
                               (tu/mock-handler-context))
      (is (= (vec (reverse @test-archiveHerateTable-results))
             [{:kausi "2019-2020" :to-table "archive_2019-2020"}
              {:kausi "2020-2021" :to-table "archive_2020-2021"}
              {:kausi "2021-2022" :to-table "archive_2021-2022"}
              {:kausi "2022-2023" :to-table "archive_2022-2023"}])))))
