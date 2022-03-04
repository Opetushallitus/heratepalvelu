(ns oph.heratepalvelu.db.dynamodb-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.db.dynamodb :as ddb]))

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


(definterface IMockAttributeValueBuilder
  (build [])
  (n [number])
  (s [string]))

(deftype MockAttributeValueBuilder [t av]
  IMockAttributeValueBuilder
  (build [this] {:type t :value av})
  (n [this number] (MockAttributeValueBuilder. "number" number))
  (s [this string] (MockAttributeValueBuilder. "string" string)))

(defn- mock-create-attribute-value-builder []
  (MockAttributeValueBuilder. nil nil))

(deftest test-to-attribute-value
  (testing "Varmista, että to-attribute-value käyttää apufunktioita oikein"
    (with-redefs [oph.heratepalvelu.db.dynamodb/create-attribute-value-builder
                  mock-create-attribute-value-builder]
      (is (= (ddb/to-attribute-value :s "asdf") {:type "string" :value "asdf"}))
      (is (= (ddb/to-attribute-value :n 12) {:type "number" :value "12"})))))


(definterface IMockConditionBuilder
  (build [])
  (attributeValueList [values])
  (comparisonOperator [operator]))

(deftype MockConditionBuilder [op stored-values]
  IMockConditionBuilder
  (build [this] {:operator op :values stored-values})
  (attributeValueList [this values]
    (MockConditionBuilder. op (or values stored-values)))
  (comparisonOperator [this operator]
    (MockConditionBuilder. (or operator op) stored-values)))

(defn- mock-create-condition-builder [] (MockConditionBuilder. nil nil))

(deftest test-build-condition
  (testing "Varmista, että build-condition toimii oikein"
    (with-redefs [oph.heratepalvelu.db.dynamodb/create-attribute-value-builder
                  mock-create-attribute-value-builder
                  oph.heratepalvelu.db.dynamodb/create-condition-builder
                  mock-create-condition-builder]
      (let [test-values-1 [:eq [:s "asdf"]]
            test-values-2 [:le [:n 123]]
            test-values-3 [:between [[:s "aaa"] [:s "ccc"]]]
            results-1 {:operator "EQ"
                       :values [{:type "string" :value "asdf"}]}
            results-2 {:operator "LE"
                       :values [{:type "number" :value "123"}]}
            results-3 {:operator "BETWEEN"
                       :values [{:type "string" :value "aaa"}
                                {:type "string" :value "ccc"}]}]
        (is (= (ddb/build-condition test-values-1) results-1))
        (is (= (ddb/build-condition test-values-2) results-2))
        (is (= (ddb/build-condition test-values-3) results-3))))))


(definterface IMockAttributeValue
  (b [])
  (bool [])
  (bs [])
  (l [])
  (m [])
  (n [])
  (ns [])
  (nul [])
  (s [])
  (ss []))

(deftype MockAttributeValue [datatype value]
  IMockAttributeValue
  (b [this] (if (= datatype "b") value nil))
  (bool [this] (if (= datatype "bool") value nil))
  (bs [this] (if (= datatype "bs") value nil))
  (l [this] (if (= datatype "l") value nil))
  (m [this] (if (= datatype "m") value nil))
  (n [this] (if (= datatype "n") value nil))
  (ns [this] (if (= datatype "ns") value nil))
  (nul [this] (if (= datatype "nul") value nil))
  (s [this] (if (= datatype "s") value nil))
  (ss [this] (if (= datatype "ss") value nil)))

(deftest test-get-value
  (testing "Varmista, että get-value toimii oikein"
    (is (= (ddb/get-value (MockAttributeValue. "n" 123)) 123))
    (is (= (ddb/get-value (MockAttributeValue. "s" "asdf")) "asdf"))))


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


(definterface IMockPutItemRequestBuilder
  (build [])
  (tableName [table])
  (item [item])
  (conditionExpression [cond-expr]))

(deftype MockPutItemRequestBuilder [contents]
  IMockPutItemRequestBuilder
  (build [this] contents)
  (tableName [this table]
    (MockPutItemRequestBuilder. (assoc contents :tableName table)))
  (item [this item]
    (MockPutItemRequestBuilder. (assoc contents :item item)))
  (conditionExpression [this cond-expr]
    (MockPutItemRequestBuilder.
      (assoc contents :conditionExpression cond-expr))))

(defn- mock-create-put-item-request-builder [] (MockPutItemRequestBuilder. {}))

(deftest test-put-item
  (testing "Varmista, että put-item toimii oikein"
    (with-redefs [environ.core/env {:herate-table "herate-table-name"}
                  oph.heratepalvelu.db.dynamodb/create-put-item-request-builder
                  mock-create-put-item-request-builder
                  oph.heratepalvelu.db.dynamodb/ddb-client (MockDDBClient.)
                  oph.heratepalvelu.db.dynamodb/map-vals-to-attribute-values
                  (fn [item] {:mapped-to-attribute-values item})]
      (let [test-item {:test-field "abc"}
            test-options {:cond-expr "a = 4"}
            results-1 {:tableName "herate-table-name"
                       :item {:mapped-to-attribute-values {:test-field "abc"}}
                       :conditionExpression "a = 4"}
            results-2 {:tableName "herate-table-name"
                       :item {:mapped-to-attribute-values {:test-field "abc"}}}]
        (ddb/put-item test-item test-options)
        (is (= {:putItem results-1} @mock-ddb-client-request-results))
        (ddb/put-item test-item {})
        (is (= {:putItem results-2} @mock-ddb-client-request-results))))))


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


(definterface IMockUpdateItemRequestBuilder
  (append [field value])
  (build [])
  (tableName [table])
  (key [key-conds])
  (updateExpression [update-expr])
  (expressionAttributeNames [expr-attr-names])
  (expressionAttributeValues [expr-attr-vals])
  (conditionExpression [cond-expr]))

(deftype MockUpdateItemRequestBuilder [contents]
  IMockUpdateItemRequestBuilder
  (append [this field value]
    (MockUpdateItemRequestBuilder. (assoc contents field value)))
  (build [this] contents)
  (tableName [this table] (.append this :tableName table))
  (key [this key-conds] (.append this :key key-conds))
  (updateExpression [this update-expr]
    (.append this :updateExpression update-expr))
  (expressionAttributeNames [this expr-attr-names]
    (.append this :expressionAttributeNames expr-attr-names))
  (expressionAttributeValues [this expr-attr-vals]
    (.append this :expressionAttributeValues expr-attr-vals))
  (conditionExpression [this cond-expr]
    (.append this :conditionExpression cond-expr)))

(deftest test-update-item
  (testing "Varmista, että update-item toimii oikein"
    (with-redefs
      [environ.core/env {:herate-table "herate-table-name"}
       oph.heratepalvelu.db.dynamodb/create-update-item-request-builder
       (fn [] (MockUpdateItemRequestBuilder. {}))
       oph.heratepalvelu.db.dynamodb/ddb-client (MockDDBClient.)]
      (let [test-key-conds {:test-field [:s "asdf"]}
            test-options {:update-expr "SET #x = :x"
                          :expr-attr-names {"#x" "XX"}
                          :expr-attr-vals {":x" [:n 6]}
                          :cond-expr "attribute_not_exists(y)"}
            results {:updateItem
                     {:updateExpression "SET #x = :x"
                      :expressionAttributeNames {"#x" "XX"}
                      :expressionAttributeValues {":x" (ddb/to-attribute-value
                                                         [:n 6])}
                      :conditionExpression "attribute_not_exists(y)"
                      :tableName "herate-table-name"
                      :key {"test-field" (ddb/to-attribute-value
                                           [:s "asdf"])}}}]
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


(definterface IMockDeleteItemRequestBuilder
  (build [])
  (tableName [table])
  (key [key-conds]))

(deftype MockDeleteItemRequestBuilder [contents]
  IMockDeleteItemRequestBuilder
  (build [this] contents)
  (tableName [this table]
    (MockDeleteItemRequestBuilder. (assoc contents :tableName table)))
  (key [this key-conds]
    (MockDeleteItemRequestBuilder. (assoc contents :key key-conds))))

(deftest test-delete-item
  (testing "Varmista, että delete-item toimii oikein"
    (with-redefs
      [environ.core/env {:herate-table "herate-table-name"}
       oph.heratepalvelu.db.dynamodb/create-delete-item-request-builder
       (fn [] (MockDeleteItemRequestBuilder. {}))
       oph.heratepalvelu.db.dynamodb/ddb-client (MockDDBClient.)]
      (let [test-key-conds {:test-field [:n 1234]}
            results {:deleteItem
                     {:tableName "herate-table-name"
                      :key {"test-field" (ddb/to-attribute-value [:n 1234])}}}]
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
