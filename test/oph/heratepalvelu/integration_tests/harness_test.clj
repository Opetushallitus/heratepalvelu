(ns oph.heratepalvelu.integration-tests.harness-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]))

(deftest test-db-mock-functions
  (testing "Varmista, että DB mock funkiot toimii odotuksenmukaisesti"
    (let [item-1 {:test-item-id [:n 12]
                  :type-and-term [:s "x/2022"]
                  :other [:s "asdf"]}
          item-2 {:test-item-id [:n 7]
                  :type-and-term [:s "abc/2020"]
                  :other [:s "0609809807"]}
          item-3 {:test-item-id [:n 56]
                  :type-and-term [:s "oiu/1999"]
                  :other [:s "lkj"]}]
      (mdb/clear-mock-db)
      (mdb/create-table "test-table" {:primary-key :test-item-id
                                      :sort-key :type-and-term})
      (mdb/set-table-contents "test-table" [item-1 item-2 item-3])
      (is (= (mdb/get-item {:test-item-id [:n 12] :type-and-term [:s "x/2022"]}
                           "test-table")
             {:test-item-id 12 :type-and-term "x/2022" :other "asdf"}))
      (mdb/put-item {:test-item-id [:n 42]
                     :type-and-term [:s "nm/2019"]
                     :other [:s "poiupo"]}
                    {:cond-expr "attribute_not_exists(test-item-id)"}
                    "test-table")
      (is (= (mdb/get-whole-table "test-table")
             {{:test-item-id [:n 12] :type-and-term [:s "x/2022"]} item-1
              {:test-item-id [:n 7] :type-and-term [:s "abc/2020"]} item-2
              {:test-item-id [:n 56] :type-and-term [:s "oiu/1999"]} item-3
              {:test-item-id [:n 42]
               :type-and-term [:s "nm/2019"]} {:test-item-id [:n 42]
                                               :type-and-term [:s "nm/2019"]
                                               :other [:s "poiupo"]}}))
      (mdb/put-item {:test-item-id [:n 42]
                     :type-and-term [:s "nm/2019"]
                     :other [:s "IIII"]}
                    {:cond-expr "attribute_not_exists(test-item-id)"}
                    "test-table")
      (is (= (mdb/get-whole-table "test-table")
             {{:test-item-id [:n 12] :type-and-term [:s "x/2022"]} item-1
              {:test-item-id [:n 7] :type-and-term [:s "abc/2020"]} item-2
              {:test-item-id [:n 56] :type-and-term [:s "oiu/1999"]} item-3
              {:test-item-id [:n 42]
               :type-and-term [:s "nm/2019"]} {:test-item-id [:n 42]
                                               :type-and-term [:s "nm/2019"]
                                               :other [:s "poiupo"]}}))
      (is (= (mdb/query-items {:test-item-id [:le [:n 43]]
                               :type-and-term [:le [:s "ww"]]}
                              {:filter-expression "#value = :value"
                               :expr-attr-names {"#value" "other"}
                               :expr-attr-vals {":value" [:s "poiupo"]}}
                              "test-table")
             [{:test-item-id 42 :type-and-term "nm/2019" :other "poiupo"}]))
      (mdb/delete-item {:test-item-id [:n 7] :type-and-term [:s "abc/2020"]}
                       "test-table")
      (is (= (mdb/get-whole-table "test-table")
             {{:test-item-id [:n 12] :type-and-term [:s "x/2022"]} item-1
              {:test-item-id [:n 56] :type-and-term [:s "oiu/1999"]} item-3
              {:test-item-id [:n 42]
               :type-and-term [:s "nm/2019"]} {:test-item-id [:n 42]
                                               :type-and-term [:s "nm/2019"]
                                               :other [:s "poiupo"]}}))
      (mdb/update-item {:test-item-id [:n 12] :type-and-term [:s "x/2022"]}
                       {:update-expr "SET #value = :value, #newVal = :newVal"
                        :expr-attr-names {"#value" "other"
                                          "#newVal" "new-field"}
                        :expr-attr-vals {":value" [:s "yyy"]
                                         ":newVal" [:s "uusi arvo"]}}
                       "test-table")
      (is (= (mdb/get-whole-table "test-table")
             {{:test-item-id [:n 12]
               :type-and-term [:s "x/2022"]} {:test-item-id [:n 12]
                                              :type-and-term [:s "x/2022"]
                                              :other [:s "yyy"]
                                              :new-field [:s "uusi arvo"]}
              {:test-item-id [:n 56]
               :type-and-term [:s "oiu/1999"]} item-3
              {:test-item-id [:n 42]
               :type-and-term [:s "nm/2019"]} {:test-item-id [:n 42]
                                               :type-and-term [:s "nm/2019"]
                                               :other [:s "poiupo"]}})))))
