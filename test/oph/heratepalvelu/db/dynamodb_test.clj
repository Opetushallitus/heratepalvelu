(ns oph.heratepalvelu.db.dynamodb-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.db.dynamodb :as ddb])
  (:import (software.amazon.awssdk.services.dynamodb DynamoDbClient)
           (software.amazon.awssdk.services.dynamodb.model Condition
                                                           DeleteItemRequest
                                                           PutItemRequest
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

(definterface IMockQueryResponse (items []))

(deftype MockQueryResponse [items-set]
  IMockQueryResponse
  (items [this] items-set))

(definterface IMockScanResponse
  (items [])
  (hasLastEvaluatedKey [])
  (lastEvaluatedKey []))

(deftype MockScanResponse [items-set]
  IMockScanResponse
  (items [this] items-set)
  (hasLastEvaluatedKey [this] true)
  (lastEvaluatedKey [this] "mock-last-evaluated-key"))

(definterface IMockGetItemResponse (item []))

(deftype MockGetItemResponse [item-data]
  IMockGetItemResponse
  (item [this] item-data))

(def mock-ddb-client-request-results (atom {}))

(definterface IMockDDBClient
  (putItem [req])
  (query [req])
  (scan [req])
  (deleteItem [req])
  (getItem [req])
  (updateItem [req]))

(deftype MockDDBClient []
  IMockDDBClient
  (putItem [this req] (reset! mock-ddb-client-request-results {:putItem req}))
  (query [this req] (MockQueryResponse. [req]))
  (scan [this req] (MockScanResponse. [req]))
  (deleteItem [this req]
    (reset! mock-ddb-client-request-results {:deleteItem req}))
  (getItem [this req] (MockGetItemResponse. req))
  (updateItem [this req]
    (reset! mock-ddb-client-request-results {:updateItem req})))

(def mockDDBClient
  (proxy [DynamoDbClient] []
    (deleteItem [^DeleteItemRequest req]
      (let [resp {:key       (.key req)
                  :tableName (.tableName req)}]
        (reset! mock-ddb-client-request-results resp)
        nil))
    (putItem [^PutItemRequest req]
      (let [resp {:conditionExpression (.conditionExpression req)
                  :item                (.item req)
                  :tableName           (.tableName req)}]
        (reset! mock-ddb-client-request-results resp)
        nil))
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

(definterface IMockQueryRequestBuilder
  (append [field value])
  (build [])
  (tableName [table])
  (keyConditions [conditions])
  (indexName [index])
  (limit [l])
  (filterExpression [filter-expression])
  (expressionAttributeNames [expr-attr-names])
  (expressionAttributeValues [expr-attr-vals]))

(deftype MockQueryRequestBuilder [contents]
  IMockQueryRequestBuilder
  (append [this field value]
    (MockQueryRequestBuilder. (assoc contents field value)))
  (build [this] {:request-body contents})
  (tableName [this table] (.append this :tableName table))
  (keyConditions [this conditions]
    (.append this :keyConditions conditions))
  (indexName [this index] (.append this :indexName index))
  (limit [this l] (.append this :limit l))
  (filterExpression [this filter-expression]
    (.append this :filterExpression filter-expression))
  (expressionAttributeNames [this expr-attr-names]
    (.append this :expressionAttributeNames expr-attr-names))
  (expressionAttributeValues [this expr-attr-vals]
    (.append this :expressionAttributeValues expr-attr-vals)))

(defn- mock-create-query-request-builder [] (MockQueryRequestBuilder. {}))

(defn- mock-map-attribute-values-to-vals [item]
  {:mapped-request-body (:request-body item)})

(defn- mock-qi-map-vals-to-conditions [item] {:mapped-vals-to-conditions item})

(deftest test-query-items
  (testing "Varmista, että query-items toimii oikein"
    (with-redefs [environ.core/env {:herate-table "herate-table-name"}
                  oph.heratepalvelu.db.dynamodb/create-query-request-builder
                  mock-create-query-request-builder
                  oph.heratepalvelu.db.dynamodb/ddb-client (MockDDBClient.)
                  oph.heratepalvelu.db.dynamodb/map-attribute-values-to-vals
                  mock-map-attribute-values-to-vals
                  oph.heratepalvelu.db.dynamodb/map-vals-to-conditions
                  mock-qi-map-vals-to-conditions]
      (let [test-key-conds {:test-field [:eq [:s "asdf"]]}
            test-options {:index "testIndex"
                          :limit 10
                          :filter-expression "#a = :a"
                          :expr-attr-names {"#a" "AAA"}
                          :expr-attr-vals {":a" [:s "aaa"]}}
            results [{:mapped-request-body
                      {:tableName "herate-table-name"
                       :keyConditions {:mapped-vals-to-conditions
                                       {:test-field [:eq [:s "asdf"]]}}
                       :indexName "testIndex"
                       :limit 10
                       :filterExpression "#a = :a"
                       :expressionAttributeNames {"#a" "AAA"}
                       :expressionAttributeValues
                       {":a" (ddb/to-attribute-value [:s "aaa"])}}}]]
        (is (= (ddb/query-items test-key-conds test-options) results))))))

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

(definterface IMockGetItemRequestBuilder
  (build [])
  (tableName [table])
  (key [key-conds]))

(deftype MockGetItemRequestBuilder [contents]
  IMockGetItemRequestBuilder
  (build [this] {:request-body contents})
  (tableName [this table]
    (MockGetItemRequestBuilder. (assoc contents :tableName table)))
  (key [this key-conds]
    (MockGetItemRequestBuilder. (assoc contents :key key-conds))))

(deftest test-get-item
  (testing "Varmista, että get-item toimii oikein"
    (with-redefs [environ.core/env {:herate-table "herate-table-name"}
                  oph.heratepalvelu.db.dynamodb/create-get-item-request-builder
                  (fn [] (MockGetItemRequestBuilder. {}))
                  oph.heratepalvelu.db.dynamodb/ddb-client (MockDDBClient.)
                  oph.heratepalvelu.db.dynamodb/map-attribute-values-to-vals
                  mock-map-attribute-values-to-vals]
      (let [test-key-conds {:test-field [:s "asdf"]}
            results {:mapped-request-body
                     {:tableName "herate-table-name"
                      :key {"test-field" (ddb/to-attribute-value
                                           [:s "asdf"])}}}]
        (is (= (ddb/get-item test-key-conds) results))))))

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

(definterface IMockScanRequestBuilder
  (build [])
  (filterExpression [filter-expression])
  (tableName [table]))

(deftype MockScanRequestBuilder [contents]
  IMockScanRequestBuilder
  (build [this] {:request-body contents})
  (filterExpression [this filter-expression]
    (MockScanRequestBuilder.
      (assoc contents :filterExpression filter-expression)))
  (tableName [this table]
    (MockScanRequestBuilder. (assoc contents :tableName table))))

(deftest test-scan
  (testing "Varmista, että scan toimii oikein"
    (with-redefs [oph.heratepalvelu.db.dynamodb/create-scan-request-builder
                  (fn [] (MockScanRequestBuilder. {}))
                  oph.heratepalvelu.db.dynamodb/ddb-client (MockDDBClient.)
                  oph.heratepalvelu.db.dynamodb/map-attribute-values-to-vals
                  mock-map-attribute-values-to-vals]
      (let [test-options {:filter-expression "a = b"}
            test-table "test-table-name"
            results {:items [{:mapped-request-body
                              {:filterExpression "a = b"
                               :tableName "test-table-name"}}]
                     :last-evaluated-key "mock-last-evaluated-key"}]
        (is (= (ddb/scan test-options test-table) results))))))
