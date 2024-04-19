(ns oph.heratepalvelu.tep.archiveNippuTable-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.tep.archiveNippuTable :as ant]
            [oph.heratepalvelu.test-util :as tu]))

(defn- mock-scan [options table] {:options options :table table})

(deftest test-do-query
  (testing "Varmista, että do-query kutsuu funktioita oikein"
    (with-redefs [environ.core/env {:nippu-table "nippu-table-name"}
                  oph.heratepalvelu.db.dynamodb/scan mock-scan]
      (is (= (ant/do-query "2021-07-02" "2022-07-01" nil)
             {:options {:filter-expression "niputuspvm BETWEEN :start AND :end"
                        :expr-attr-vals    {":start" [:s "2021-07-02"]
                                            ":end"   [:s "2022-07-01"]}}
              :table   "nippu-table-name"}))
      (is (= (ant/do-query "2021-07-02" "2022-07-01" "asdf")
             {:options {:filter-expression "niputuspvm BETWEEN :start AND :end"
                        :expr-attr-vals      {":start" [:s "2021-07-02"]
                                              ":end"   [:s "2022-07-01"]}
                        :exclusive-start-key "asdf"}
              :table   "nippu-table-name"})))))

(def results (atom []))

(defn- mock-do-query [kausi-start kausi-end last-evaluated-key]
  (reset! results (cons {:type               "mock-do-query"
                         :kausi-start        kausi-start
                         :kausi-end          kausi-end
                         :last-evaluated-key last-evaluated-key}
                        @results))
  {:items [{:ohjaaja_ytunnus_kj_tutkinto "oykt"
            :niputuspvm                  "2021-12-20"
            :other_field                 "asdf"}]})

(defn- mock-put-item [key-conds options table]
  (reset! results (cons {:type "mock-put-item"
                         :key-conds key-conds
                         :options options
                         :table table}
                        @results)))

(defn- mock-delete-item [key-conds table]
  (reset! results (cons {:type "mock-delete-item"
                         :key-conds key-conds
                         :table table}
                        @results)))

(deftest test-do-nippu-table-archiving
  (testing "Varmista, että do-nippu-table-archiving toimii oikein"
    (with-redefs [environ.core/env {:nippu-table "nippu-table-name"}
                  oph.heratepalvelu.db.dynamodb/delete-item mock-delete-item
                  oph.heratepalvelu.db.dynamodb/put-item mock-put-item
                  oph.heratepalvelu.tep.archiveNippuTable/do-query
                  mock-do-query]
      (ant/do-nippu-table-archiving "2021-07-02" "2022-07-01" "to-table-name")
      (is (= (vec (reverse @results))
             [{:type               "mock-do-query"
               :kausi-start        "2021-07-02"
               :kausi-end          "2022-07-01"
               :last-evaluated-key nil}
              {:type      "mock-put-item"
               :key-conds {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt"]
                           :niputuspvm                  [:s "2021-12-20"]
                           :other_field                 [:s "asdf"]}
               :options   {}
               :table     "to-table-name"}
              {:type "mock-delete-item"
               :key-conds {:ohjaaja_ytunnus_kj_tutkinto [:s "oykt"]
                           :niputuspvm                  [:s "2021-12-20"]}
               :table     "nippu-table-name"}])))))

(def test-archiveNippuTable-results (atom []))

(defn- mock-do-nippu-table-archiving [kausi-start kausi-end to-table]
  (reset! test-archiveNippuTable-results
          (cons {:kausi-start kausi-start
                 :kausi-end kausi-end
                 :to-table to-table}
                @test-archiveNippuTable-results)))

(deftest test-archiveNippuTable
  (testing "Varmista, että -archiveNippuTable tekee oikeita kutsuja"
    (with-redefs
      [environ.core/env {:archive-table-2021-2022 "archive-21-22"
                         :archive-table-2022-2023 "archive-22-23"}
       oph.heratepalvelu.tep.archiveNippuTable/do-nippu-table-archiving
       mock-do-nippu-table-archiving]
      (ant/-archiveNippuTable {}
                              (tu/mock-handler-event :scheduledherate)
                              (tu/mock-handler-context))
      (is (= (vec (reverse @test-archiveNippuTable-results))
             [{:kausi-start "2021-07-02"
               :kausi-end "2022-07-01"
               :to-table "archive-21-22"}
              {:kausi-start "2022-07-02"
               :kausi-end "2023-07-01"
               :to-table "archive-22-23"}])))))
