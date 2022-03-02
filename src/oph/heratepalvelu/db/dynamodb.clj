(ns oph.heratepalvelu.db.dynamodb
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]])
  (:import (clojure.lang Reflector)
           (software.amazon.awssdk.services.dynamodb DynamoDbClient)
           (software.amazon.awssdk.services.dynamodb.model AttributeValue
                                                           Condition
                                                           DeleteItemRequest
                                                           GetItemRequest
                                                           PutItemRequest
                                                           QueryRequest
                                                           ScanRequest
                                                           UpdateItemRequest)
           (software.amazon.awssdk.regions Region)
           (software.amazon.awssdk.core.util DefaultSdkAutoConstructMap
                                             DefaultSdkAutoConstructList)
           (software.amazon.awssdk.core.client.config
             ClientOverrideConfiguration)
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

(defn- invoke-instance-method
  "Kutsuu Java Objektin methodia nimeltään."
  [inst m params]
  (Reflector/invokeInstanceMethod
    inst m (to-array params)))

(defn- create-attribute-value-builder
  "Abstraktio AttributeValue/builderin ympäri, joka helpottaa testaamista."
  []
  (AttributeValue/builder))

(defn to-attribute-value
  "Muuttaa [:<tyyppi> <arvo>] -tyyppisen arvo AttributeValue-objektiksi."
  ([tk v]
   (if-let [t (get attribute-types tk)]
     (.build (invoke-instance-method
               (create-attribute-value-builder) t [(if (= t "n") (str v) v)]))
     (throw (Exception. (str "Unknown attribute type " tk)))))
  ([[tk v]]
   (to-attribute-value tk v)))

(defn- create-condition-builder
  "Abstraktio Condition/builderin ympäri, joka helpottaa testaamista."
  []
  (Condition/builder))

(defn build-condition
  "Muuttaa konditionaalisen hakuavaimen muodoista [:<op> [:<tyyppi> <arvo>]]
  tai [:<op> [[:<tyyppi> <arvo>] [:<tyyppi> <arvo]]] Condition-objektiksi."
  [op-vals]
  (let [[op orig-values] op-vals
        values (if (coll? (first orig-values))
                 (map to-attribute-value orig-values)
                 [(to-attribute-value orig-values)])]
    (-> (create-condition-builder)
        (.attributeValueList values)
        (.comparisonOperator (get comparison-operators op "EQ"))
        (.build))))

(defn map-vals-to-attribute-values
  "Muuttaa key value mapin muodosta {:<key> [:<tyyppi> <arvo>]} muodoksi
  {\"<key>\" <AttributeValue>}."
  [av-map]
  (into {} (for [[k [t v]] av-map] [(name k) (to-attribute-value t v)])))

(defn map-vals-to-conditions
  "Muuttaa key value mapin muodosta {:<key> [:<op> [:<tyyppi> <arvo>]]} muodoksi
  {\"<key>\" <Condition>}."
  [vals-map]
  (into {} (for [[k v] vals-map] [(name k) (build-condition v)])))

(defn get-value
  "Hakee arvon AttributeValue-objektista ja muuttaa sen oikeaksi
  Clojure-tyypiksi."
  [av]
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

(defn map-attribute-values-to-vals
  "Muuttaa key value mapin muodosta {\"<key>\" <AttributeValue>} muodoksi
  {:<key> <Clojure-arvo>}."
  [item]
  (reduce-kv #(assoc %1 (keyword %2) (get-value %3))
             {} (into {} item)))

(defn- create-put-item-request-builder
  "Abstraktio PutItemRequest/builderin ympäri, joka helpottaa testaamista."
  []
  (PutItemRequest/builder))

(defn put-item
  "Tallentaa yhden tietueen tietokantaan. Tallennettavassa itemissa täytyy olla
  primary key ja sort key (jos taulussa on sort key), ja options-objekti voi
  sisältää seuraavat ehdolliset kentät:
    :cond-expr - konditionaalinen ekspressio DynamoDB:n syntaksin mukaan."
  ([item options]
    (put-item item options (:herate-table env)))
  ([item options table]
   (.putItem ddb-client (-> (create-put-item-request-builder)
                            (.tableName table)
                            (.item (map-vals-to-attribute-values item))
                            (cond->
                              (:cond-expr options)
                              (.conditionExpression (:cond-expr options)))
                            (.build)))))

(defn- create-query-request-builder
  "Abstraktio QueryRequest/builderin ympäri, joka helpottaa testaamista."
  []
  (QueryRequest/builder))

(defn query-items
  "Hakee tietueita tiettyjen ehtojen perusteella. Ehdot, jotka koskevat primary
  keytä ja sort keytä, ilmaistaan key-conds -argumentissa noilla keywordeilla,
  jotka löytyvät comparison-operators -objektista. Funktion key-conds
  -argumentti seuraa tällaista syntaksia:
    {:<key> [:<op> [:<tyyppi> <value>]]}
  
  Objektista options voi löytyä myös suodatusekspressio ja/tai muita ehdollisia
  parametreja:
    :index               - index, jota käytetään key-condsissa
    :limit               - maksimimäärä palautettavia tietueita
    :filter-expression   - DynamoDB-formaatissa oleva predikaatti, jonka
                           perusteella palautettavat tietueet suodatetaan
    :expr-attr-names     - lista avaimista ja kentännimiä, joilla avaimet
                           korvataan filter-expressionissa
    :expr-attr-vals      - lista avaimista ja attribuuttiarvoista, jotka
                           laitetaan avaimien sijoihin, kun ekspressio
                           evaluoidaan"
  ([key-conds options]
    (query-items key-conds options (:herate-table env)))
  ([key-conds options table]
   (let [conditions (map-vals-to-conditions key-conds)
         response (.query ddb-client (-> (create-query-request-builder)
                                         (.tableName table)
                                         (.keyConditions conditions)
                                         (cond->
                                           (:index options)
                                           (.indexName (:index options))
                                           (:limit options)
                                           (.limit (int (:limit options)))
                                           (:filter-expression options)
                                           (.filterExpression
                                             (:filter-expression options))
                                           (:expr-attr-names options)
                                           (.expressionAttributeNames
                                             (:expr-attr-names options))
                                           (:expr-attr-vals options)
                                           (.expressionAttributeValues
                                             (map-vals-to-attribute-values
                                               (:expr-attr-vals options))))
                                         (.build)))
         items (.items response)]
     (into [] (map
                map-attribute-values-to-vals
                items)))))

(defn- create-update-item-request-builder
  "Abstraktio UpdateItemRequest/builderin ympäri, joka helpottaa testaamista."
  []
  (UpdateItemRequest/builder))

(defn update-item
  "Päivittää yhden tietueen tietokantaan. Tietue identifioidaan key-condsissa
  annettujen primary keyn ja sort keyn perusteella, jotka seuraavat tätä mallia:
    {:<key> [:<tyyppi> <value>]}

  Objektiin options voi asettaa myös seuraavat parametrit, joista ensimmäinen on
  pakollinen:
    :update-expr      - pakollinen kenttä, jossa määritellään ne tietuekentät,
                        jotka päivitetään. Syntaksi muistuttaa SQL SET-komentoa.
    :cond-expr        - DynamoDB-formaattinen ekspressio, joka evaluoidaan ennen
                        päivitystä tietokantaan. Jos se palauttaa false,
                        päivitystä ei tehdä.
    :expr-attr-names  - lista avaimista ja kentännimiä, joilla avaimet korvataan
                        update-exprissa ja cond-exprissa
    :expr-attr-vals   - lista avaimista ja attribuuttiarvoista, jotka laitetaan
                        avaimien sijoihin, kun ekspressio (update-expr tai
                        cond-expr) evaluoidaan"
  ([key-conds options]
    (update-item key-conds options (:herate-table env)))
  ([key-conds options table]
   (let [req (-> (create-update-item-request-builder)
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

(defn- create-get-item-request-builder
  "Abstraktio GetItemRequest/builderin ympäri, joka helpottaa testaamista."
  []
  (GetItemRequest/builder))

(defn get-item
  "Hakee yhden tietueen tietokannasta key-condsin perusteella. Avaimien syntaksi
  seuraa tätä mallia:
    {:<key> [:<tyyppi> <arvo>]}"
  ([key-conds]
    (get-item key-conds (:herate-table env)))
  ([key-conds table]
    (let [req (-> (create-get-item-request-builder)
                  (.tableName table)
                  (.key (map-vals-to-attribute-values key-conds))
                  (.build))
          response (.getItem ddb-client req)
          item (.item response)]
      (map-attribute-values-to-vals item))))

(defn- create-delete-item-request-builder
  "Abstraktio DeleteItemRequest/builderin ympäri, joka helpottaa testaamista."
  []
  (DeleteItemRequest/builder))

(defn delete-item
  "Poistaa yhden tietueen tietokannasta key-condsin perusteella. Avaimien
  syntaksi seuraa tätä mallia:
    {:<key> [:<tyyppi> <arvo>]}"
  ([key-conds]
    (delete-item key-conds (:herate-table env)))
  ([key-conds table]
    (let [req (-> (create-delete-item-request-builder)
                  (.tableName table)
                  (.key (map-vals-to-attribute-values key-conds))
                  (.build))]
      (.deleteItem ddb-client req))))

(defn- create-scan-request-builder
  "Abstraktio ScanRequest/builderin ympäri, joka helpottaa testaamista."
  []
  (ScanRequest/builder))

(defn scan
  "Käy taulussa olevien tietueiden läpi useat kerralla jossakin järjestyksessä.
  Jos :exclusive-start-key annetaan optiona, scan alkaa sieltä; muuten se alkaa
  tietueiden alusta. Voi antaa seuraavat optiot:
    :exclusive-start-key  - määrittää paikan, josta scan alkaa tietokannassa
    :filter-expression    - predikaatti, joka evaluoidaan jokaisesta tietueesta;
                            tietue poistetaan palautettavasta listasta, jos tämä
                            predikaatti palauttaa false
    :expr-attr-names      - lista avaimista ja kentännimiä, joilla avaimet
                            korvataan filter-expressionissa
    :expr-attr-vals       - lista avaimista ja attribuuttiarvoista, jotka
                            laitetaan avaimien sijoihin, kun ekspressio
                            evaluoidaan"
  [options table]
  (let [req (-> (create-scan-request-builder)
                (cond->
                  (:filter-expression options)
                  (.filterExpression (:filter-expression options))
                  (:exclusive-start-key options)
                  (.exclusiveStartKey (:exclusive-start-key options))
                  (:expr-attr-names options)
                  (.expressionAttributeNames
                    (:expr-attr-names options))
                  (:expr-attr-vals options)
                  (.expressionAttributeValues
                    (map-vals-to-attribute-values (:expr-attr-vals options))))
                (.tableName table)
                (.build))
        response (.scan ddb-client req)
        items (.items response)]
    {:items (into [] (map map-attribute-values-to-vals items))
     :last-evaluated-key (when (.hasLastEvaluatedKey response)
                           (.lastEvaluatedKey response))}))
