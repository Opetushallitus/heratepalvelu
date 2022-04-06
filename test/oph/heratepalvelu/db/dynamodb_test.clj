(ns oph.heratepalvelu.db.dynamodb-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.db.dynamodb :as ddb])
  (:import (software.amazon.awssdk.services.dynamodb DynamoDbClient)
           (software.amazon.awssdk.services.dynamodb.model Condition
                                                           DeleteItemRequest
                                                           GetItemRequest
                                                           GetItemResponse
                                                           PutItemRequest
                                                           QueryRequest
                                                           QueryResponse
                                                           ScanRequest
                                                           ScanResponse
                                                           UpdateItemRequest)))

(defn- mock-to-attribute-value [tk v] {:key tk :value v})

(deftest test-map-vals-to-attribute-values
  (testing "Varmista, että map-vals-to-attribute-values toimii oikein"
    (with-redefs [oph.heratepalvelu.db.dynamodb/to-attribute-value
                  mock-to-attribute-value]
      (let [test-values {:test-key [:s "ASDF"]
                         :another-option [:n 42]}
            results {"test-key" {:key :s :value "ASDF"}
                     "another-option" {:key :n :value 42}}]
        (is (= (ddb/map-vals-to-attribute-values test-values) results))))))

(defn- mock-build-condition [op-vals] {:op-vals op-vals})

(deftest test-map-vals-to-conditions
  (testing "Varmista, että map-vals-to-conditions toimii oikein"
    (with-redefs [oph.heratepalvelu.db.dynamodb/build-condition
                  mock-build-condition]
      (let [test-values {:condition-1 [:eq [:s "asdf"]]
                         :condition-2 [:between [[:s "a"] [:s "b"]]]}
            results {"condition-1" {:op-vals [:eq [:s "asdf"]]}
                     "condition-2" {:op-vals [:between [[:s "a"] [:s "b"]]]}}]
        (is (= (ddb/map-vals-to-conditions test-values) results))))))

(defn- mock-get-value [av] {:gotten-value av})

(deftest test-map-attribute-values-to-vals
  (testing "Varmista, että map-attribute-values-to-vals toimii oikein"
    (with-redefs [oph.heratepalvelu.db.dynamodb/get-value mock-get-value]
      (let [test-values {"key-one" "asdf"
                         "key-two" 42}
            results {:key-one {:gotten-value "asdf"}
                     :key-two {:gotten-value 42}}]
        (is (= (ddb/map-attribute-values-to-vals test-values) results))))))

(deftest test-map-raw-vals-to-typed-vals
  (testing "Varmista, että map-raw-vals-to-typed-vals toimii oikein"
    (let [test-item {:bool-field   false
                     :int-field    10
                     :string-field "asdf"}
          results {:bool-field   [:bool false]
                   :int-field    [:n 10]
                   :string-field [:s "asdf"]}]
      (is (= (ddb/map-raw-vals-to-typed-vals test-item) results)))))

(deftest test-to-attribute-value
  (testing "Varmista, että to-attribute-value käyttää apufunktioita oikein"
    (is (= (.s (ddb/to-attribute-value :s "asdf")) "asdf"))
    (is (= (.n (ddb/to-attribute-value :n 12)) "12"))))

(defn- extract-condition-data [^Condition condition]
  {:operator (str (.comparisonOperator condition))
   :values (vec (map ddb/get-value (.attributeValueList condition)))})

(deftest test-build-condition
  (testing "Varmista, että build-condition toimii oikein"
    (let [test1 [:eq [:s "asdf"]]
          test2 [:le [:n 123]]
          test3 [:between [[:s "aaa"] [:s "ccc"]]]
          results1 {:operator "EQ" :values ["asdf"]}
          results2 {:operator "LE" :values [123]}
          results3 {:operator "BETWEEN" :values ["aaa" "ccc"]}]
      (is (= (extract-condition-data (ddb/build-condition test1)) results1))
      (is (= (extract-condition-data (ddb/build-condition test2)) results2))
      (is (= (extract-condition-data (ddb/build-condition test3)) results3)))))

(deftest test-get-value
  (testing "Varmista, että get-value toimii oikein"
    (is (= (ddb/get-value (ddb/to-attribute-value :n 123)) 123))
    (is (= (ddb/get-value (ddb/to-attribute-value :s "asdf")) "asdf"))))

(def mock-ddb-client-request-results (atom {}))

(def mockDDBClient
  (proxy [DynamoDbClient] []
    (deleteItem [^DeleteItemRequest req]
      (let [resp {:key       (.key req)
                  :tableName (.tableName req)}]
        (reset! mock-ddb-client-request-results resp)
        nil))
    (getItem [^GetItemRequest req]
      (let [resp {:key       (.key req)
                  :tableName (.tableName req)}]
        (reset! mock-ddb-client-request-results resp)
        (.build (.item (GetItemResponse/builder)
                       {:field (ddb/to-attribute-value [:s "qwerty"])
                        :other-field (ddb/to-attribute-value [:n 5])}))))
    (putItem [^PutItemRequest req]
      (let [resp {:conditionExpression (.conditionExpression req)
                  :item                (.item req)
                  :tableName           (.tableName req)}]
        (reset! mock-ddb-client-request-results resp)
        nil))
    (query [^QueryRequest req]
      (let [resp {:expressionAttributeNames  (.expressionAttributeNames req)
                  :expressionAttributeValues (.expressionAttributeValues req)
                  :filterExpression          (.filterExpression req)
                  :indexName                 (.indexName req)
                  :limit                     (.limit req)
                  :keyConditions             (.keyConditions req)
                  :tableName                 (.tableName req)}]
        (reset! mock-ddb-client-request-results resp)
        (.build (.items (QueryResponse/builder)
                        [{"field" (ddb/to-attribute-value [:s "asdf"])}]))))
    (scan [^ScanRequest req]
      (let [resp {:exclusiveStartKey         (.exclusiveStartKey req)
                  :expressionAttributeNames  (.expressionAttributeNames req)
                  :expressionAttributeValues (.expressionAttributeValues req)
                  :filterExpression          (.filterExpression req)
                  :tableName                 (.tableName req)}]
        (reset! mock-ddb-client-request-results resp)
        (.build (.items (ScanResponse/builder)
                        [{"field" (ddb/to-attribute-value [:s "asdf"])}]))))
    (updateItem [^UpdateItemRequest req]
      (let [resp {:conditionExpression       (.conditionExpression req)
                  :expressionAttributeNames  (.expressionAttributeNames req)
                  :expressionAttributeValues (.expressionAttributeValues req)
                  :key                       (.key req)
                  :tableName                 (.tableName req)
                  :updateExpression          (.updateExpression req)}]
        (reset! mock-ddb-client-request-results resp)
        nil))))

(deftest test-put-item
  (testing "Varmista, että put-item toimii oikein"
    (with-redefs [environ.core/env {:herate-table "herate-table-name"}
                  oph.heratepalvelu.db.dynamodb/ddb-client mockDDBClient
                  oph.heratepalvelu.db.dynamodb/map-vals-to-attribute-values
                  (fn [item] {:mapped-to-attribute-values item})]
      (let [test-item {:test-field "abc"}
            test-options {:cond-expr "a = 4"}
            results-1 {:tableName "herate-table-name"
                       :item {:mapped-to-attribute-values {:test-field "abc"}}
                       :conditionExpression "a = 4"}
            results-2 {:tableName "herate-table-name"
                       :item {:mapped-to-attribute-values {:test-field "abc"}}
                       :conditionExpression nil}]
        (ddb/put-item test-item test-options)
        (is (= results-1 @mock-ddb-client-request-results))
        (ddb/put-item test-item {})
        (is (= results-2 @mock-ddb-client-request-results))))))

(deftest test-query-items
  (testing "Varmista, että query-items toimii oikein"
    (with-redefs [environ.core/env {:herate-table "herate-table-name"}
                  oph.heratepalvelu.db.dynamodb/ddb-client mockDDBClient]
      (let [test-key-conds {:test-field [:eq [:s "asdf"]]}
            test-options {:index "testIndex"
                          :limit 10
                          :filter-expression "#a = :a"
                          :expr-attr-names {"#a" "AAA"}
                          :expr-attr-vals {":a" [:s "aaa"]}}
            results {:tableName "herate-table-name"
                     :keyConditions {"test-field" (ddb/build-condition
                                                    [:eq [:s "asdf"]])}
                     :indexName "testIndex"
                     :limit 10
                     :filterExpression "#a = :a"
                     :expressionAttributeNames {"#a" "AAA"}
                     :expressionAttributeValues
                     {":a" (ddb/to-attribute-value [:s "aaa"])}}
            expected-items [{:field "asdf"}]]
        (is (= (ddb/query-items test-key-conds test-options) expected-items))
        (is (= @mock-ddb-client-request-results results))))))

(deftest test-update-item
  (testing "Varmista, että update-item toimii oikein"
    (with-redefs
      [environ.core/env {:herate-table "herate-table-name"}
       oph.heratepalvelu.db.dynamodb/ddb-client mockDDBClient]
      (let [test-key-conds {:test-field [:s "asdf"]}
            test-options {:update-expr "SET #x = :x"
                          :expr-attr-names {"#x" "XX"}
                          :expr-attr-vals {":x" [:n 6]}
                          :cond-expr "attribute_not_exists(y)"}
            results {:updateExpression "SET #x = :x"
                     :expressionAttributeNames {"#x" "XX"}
                     :expressionAttributeValues {":x" (ddb/to-attribute-value
                                                        [:n 6])}
                     :conditionExpression "attribute_not_exists(y)"
                     :tableName "herate-table-name"
                     :key {"test-field" (ddb/to-attribute-value [:s "asdf"])}}]
        (ddb/update-item test-key-conds test-options)
        (is (= results @mock-ddb-client-request-results))))))

(deftest test-get-item
  (testing "Varmista, että get-item toimii oikein"
    (with-redefs [environ.core/env {:herate-table "herate-table-name"}
                  oph.heratepalvelu.db.dynamodb/ddb-client mockDDBClient]
      (let [test-key-conds {:test-field [:s "asdf"]}
            results {:tableName "herate-table-name"
                     :key {"test-field" (ddb/to-attribute-value [:s "asdf"])}}]
        (is (= (ddb/get-item test-key-conds) {:field "qwerty" :other-field 5}))
        (is (= results @mock-ddb-client-request-results))))))

(deftest test-delete-item
  (testing "Varmista, että delete-item toimii oikein"
    (with-redefs
      [environ.core/env {:herate-table "herate-table-name"}
       oph.heratepalvelu.db.dynamodb/ddb-client mockDDBClient]
      (let [test-key-conds {:test-field [:n 1234]}
            results {:tableName "herate-table-name"
                     :key {"test-field" (ddb/to-attribute-value [:n 1234])}}]
        (ddb/delete-item test-key-conds)
        (is (= results @mock-ddb-client-request-results))))))

(deftest test-scan
  (testing "Varmista, että scan toimii oikein"
    (with-redefs [oph.heratepalvelu.db.dynamodb/ddb-client mockDDBClient]
      (let [test-options {:filter-expression "a = b"}
            test-table "test-table-name"
            results {:exclusiveStartKey         {}
                     :expressionAttributeNames  {}
                     :expressionAttributeValues {}
                     :filterExpression          "a = b"
                     :tableName                 "test-table-name"}
            expected-items {:items [{:field "asdf"}]
                            :last-evaluated-key nil}]
        (is (= (ddb/scan test-options test-table) expected-items))
        (is (= results @mock-ddb-client-request-results))))))
