(ns oph.heratepalvelu.db.dynamodb
  (:require [environ.core :refer [env]])
  (:import (clojure.lang Reflector)
           (software.amazon.awssdk.services.dynamodb DynamoDbClient)
           (software.amazon.awssdk.services.dynamodb.model PutItemRequest
                                                           QueryRequest
                                                           UpdateItemRequest
                                                           Condition
                                                           AttributeValue
                                                           GetItemRequest
                                                           ScanRequest)
           (software.amazon.awssdk.regions Region)
           (software.amazon.awssdk.core.util DefaultSdkAutoConstructMap DefaultSdkAutoConstructList)
           (software.amazon.awssdk.core.client.config ClientOverrideConfiguration)
           (com.amazonaws.xray.interceptors TracingInterceptor)))

(def ddb-client (-> (DynamoDbClient/builder)
                    (.region (Region/EU_WEST_1))
                    (.overrideConfiguration
                      (-> (ClientOverrideConfiguration/builder)
                          (.addExecutionInterceptor (TracingInterceptor.))
                          (.build)))
                    (.build)))

(def ^:private comparison-operators
  {:eq "EQ" :ne "NE" :le "LE"
   :lt "LT" :ge "GE" :gt "GT"
   :not-null "NOT_NULL" :null "NULL"
   :contains "CONTAINS" :not-contains "NOT_CONTAINS"
   :begins "BEGINS_WITH" :in "IN" :between "BETWEEN"})

(def ^:private attribute-types
  {:s "s" :n "n" :b "b"
   :ss "ss" :ns "ns" :bs "bs"
   :m "m" :l "l" :bool "bool"
   :nul "nul"})

(defn- invoke-instance-method [inst m params]
  (Reflector/invokeInstanceMethod
    inst m (to-array params)))

(defn- to-attribute-value
  ([tk v]
   (if-let [t (get attribute-types tk)]
     (.build (invoke-instance-method
               (AttributeValue/builder) t [(if (= t "n") (str v) v)]))
     (throw (Exception. (str "Unknown attribute type " tk)))))
  ([[tk v]]
   (to-attribute-value tk v)))

(defn- build-condition [op-vals]
  (let [[op vals] op-vals
        values (if (coll? (first vals))
                 (map to-attribute-value vals)
                 [(to-attribute-value vals)])]
    (-> (Condition/builder)
        (.attributeValueList values)
        (.comparisonOperator (get comparison-operators op "EQ"))
        (.build))))

(defn- map-vals-to-attribute-values [map]
  (into {} (for [[k [t v]] map] [(name k) (to-attribute-value t v)])))

(defn- map-vals-to-conditions [map]
  (into {} (for [[k v] map] [(name k) (build-condition v)])))

(defn- get-value [av]
  (reduce (fn [o t]
            (let [v (invoke-instance-method av t [])]
              (when (and (some? v)
                         (not (instance? DefaultSdkAutoConstructMap v))
                         (not (instance? DefaultSdkAutoConstructList v)))
                (reduced (cond
                           (= t "n")
                           (Integer. v)
                           (= t "ns")
                           (map #(Integer. %) v)
                           (= t "m")
                           (reduce-kv #(assoc %1 (keyword %2) (get-value %3))
                                      {} (into {} v))
                           :else v)))))
          nil (vals attribute-types)))

(defn- map-attribute-values-to-vals [item]
  (reduce-kv #(assoc %1 (keyword %2) (get-value %3))
             {} (into {} item)))

(defn put-item
  ([item options]
    (put-item item options (:herate-table env)))
  ([item options table]
   (.putItem ddb-client (-> (PutItemRequest/builder)
                            (.tableName table)
                            (.item (map-vals-to-attribute-values item))
                            (cond->
                              (:cond-expr options)
                              (.conditionExpression (:cond-expr options)))
                            (.build)))))

(defn query-items
  ([key-conds options]
    (query-items key-conds options (:herate-table env)))
  ([key-conds options table]
   (let [conditions (map-vals-to-conditions key-conds)
         response (.query ddb-client (-> (QueryRequest/builder)
                                         (.tableName table)
                                         (.keyConditions conditions)
                                         (cond->
                                           (:index options)
                                           (.indexName (:index options))
                                           (:limit options)
                                           (.limit (int (:limit options)))
                                           (:filter-expression options)
                                           (.filterExpression (:filter-expression options))
                                           (:expr-attr-names options)
                                           (.expressionAttributeNames
                                             (:expr-attr-names options))
                                           (:expr-attr-vals options)
                                           (.expressionAttributeValues
                                             (map-vals-to-attribute-values (:expr-attr-vals options))))
                                         (.build)))
         items (.items response)]
     (into [] (map
                map-attribute-values-to-vals
                items)))))

(defn update-item
  ([key-conds options]
    (update-item key-conds options (:herate-table env)))
  ([key-conds options table]
   (let [req (-> (UpdateItemRequest/builder)
                 (.tableName table)
                 (.key (map-vals-to-attribute-values key-conds))
                 (.updateExpression (:update-expr options))
                 (cond->
                   (:expr-attr-names options)
                   (.expressionAttributeNames
                     (:expr-attr-names options))
                   (:expr-attr-vals options)
                   (.expressionAttributeValues
                     (map-vals-to-attribute-values (:expr-attr-vals options)))
                   (:cond-expr options)
                   (.conditionExpression
                     (:cond-expr options)))
                 (.build))]
     (.updateItem ddb-client req))))

(defn get-item
  ([key-conds]
    (get-item key-conds (:herate-table env)))
  ([key-conds table]
    (let [req (-> (GetItemRequest/builder)
                  (.tableName table)
                  (.key (map-vals-to-attribute-values key-conds))
                  (.build))
          response (.getItem ddb-client req)
          item (.item response)]
      (map-attribute-values-to-vals item))))

(defn scan [options table]
  (let [req (-> (ScanRequest/builder)
                (cond->
                  (:filter-expression options)
                  (.filterExpression (:filter-expression options)))
                (.tableName table)
                (.build))
        response (.scan ddb-client req)
        items (.items response)]
    (into [] (map
               map-attribute-values-to-vals
               items))))
