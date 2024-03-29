(ns oph.heratepalvelu.db.dynamodb
  "Funktiot, joilla päivitetään tietokanta ja haetaan siitä tietoja."
  (:require [environ.core :refer [env]])
  (:import (clojure.lang Reflector)
           (software.amazon.awssdk.services.dynamodb DynamoDbClient)
           (software.amazon.awssdk.services.dynamodb.model
             AttributeValue
             AttributeValue$Builder
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

(def ^DynamoDbClient ddb-client
  "DynamoDB client -objekti."
  (-> (DynamoDbClient/builder)
      (.region (Region/EU_WEST_1))
      (.overrideConfiguration
        (-> (ClientOverrideConfiguration/builder)
            (.addExecutionInterceptor (TracingInterceptor.))
            ^ClientOverrideConfiguration (.build)))
      (.build)))

(def ^:private comparison-operators
  "Vertailuoperaatorit tietokantahakuja varten."
  {:eq "EQ" :ne "NE" :le "LE"
   :lt "LT" :ge "GE" :gt "GT"
   :not-null "NOT_NULL" :null "NULL"
   :contains "CONTAINS" :not-contains "NOT_CONTAINS"
   :begins "BEGINS_WITH" :in "IN" :between "BETWEEN"})

(def ^:private attribute-types
  "Tietotyypit DynamoDB-tietokannassa."
  {:s "s" :n "n" :b "b"
   :ss "ss" :ns "ns" :bs "bs"
   :m "m" :l "l" :bool "bool"
   :nul "nul"})

(defn- invoke-instance-method
  "Kutsuu Java Objektin methodia nimeltään."
  [inst m params]
  (Reflector/invokeInstanceMethod
    inst m (to-array params)))

(defn to-attribute-value
  "Muuttaa [:<tyyppi> <arvo>] -tyyppisen arvo AttributeValue-objektiksi."
  (^AttributeValue [tk v]
   (if-let [t (get attribute-types tk)]
     (.build ^AttributeValue$Builder (invoke-instance-method
                                       (AttributeValue/builder)
                                       t
                                       [(if (= t "n") (str v) v)]))
     (throw (Exception. (str "Unknown attribute type " tk)))))
  ([[tk v]]
   (to-attribute-value tk v)))

(defn build-condition
  "Muuttaa konditionaalisen hakuavaimen muodoista [:<op> [:<tyyppi> <arvo>]]
  tai [:<op> [[:<tyyppi> <arvo>] [:<tyyppi> <arvo]]] Condition-objektiksi."
  [op-vals]
  (let [[op orig-values] op-vals
        values (if (coll? (first orig-values))
                 (map to-attribute-value orig-values)
                 [(to-attribute-value orig-values)])]
    (-> (Condition/builder)
        (.attributeValueList ^clojure.lang.PersistentVector values)
        (.comparisonOperator ^String (get comparison-operators op "EQ"))
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

(declare map-attribute-values-to-vals)

(defn get-value
  "Hakee arvon AttributeValue-objektista ja muuttaa sen oikeaksi
  Clojure-tyypiksi."
  [av]
  (first
    (keep (fn [t]
            (let [v (invoke-instance-method av t [])]
              (when (and (some? v)
                         (not (instance? DefaultSdkAutoConstructMap v))
                         (not (instance? DefaultSdkAutoConstructList v)))
                (cond (= t "n") (Integer/parseInt v)
                      (= t "ns") (map #(Integer/parseInt %) v)
                      (= t "m") (map-attribute-values-to-vals v)
                      :else v))))
          (vals attribute-types))))

(defn map-attribute-values-to-vals
  "Muuttaa key value mapin muodosta {\"<key>\" <AttributeValue>} muodoksi
  {:<key> <Clojure-arvo>}."
  [item]
  (reduce-kv #(assoc %1 (keyword %2) (get-value %3)) {} (into {} item)))

(defn map-raw-vals-to-typed-vals
  "Muuttaa key value mapin muodosta {\"key\" <Clojure-arvo>} muodoksi
  {\"key\" [:<tyyppi> <arvo>]}."
  [item]
  ; FIXME: map-values
  (reduce #(assoc %1
                  (first %2) (cond (or (= (type (second %2)) java.lang.Long)
                                       (= (type (second %2)) java.lang.Integer))
                                   [:n (second %2)]
                                   (= (type (second %2)) java.lang.Boolean)
                                   [:bool (second %2)]
                                   :else [:s (second %2)]))
          {}
          (seq item)))

(defn put-item
  "Tallentaa yhden tietueen tietokantaan. Tallennettavassa itemissa täytyy olla
  primary key ja sort key (jos taulussa on sort key), ja options-objekti voi
  sisältää seuraavat ehdolliset kentät:
    :cond-expr - konditionaalinen ekspressio DynamoDB:n syntaksin mukaan."
  ([item options]
   (put-item item options (:herate-table env)))
  ([item options table]
   (.putItem ddb-client (-> (PutItemRequest/builder)
                            (.tableName table)
                            (.item (map-vals-to-attribute-values item))
                            (cond->
                              (:cond-expr options)
                              (.conditionExpression (:cond-expr options))
                              (:expr-attr-names options)
                              (.expressionAttributeNames
                                (:expr-attr-names options))
                              (:expr-attr-vals options)
                              (.expressionAttributeValues
                                (map-vals-to-attribute-values
                                  (:expr-attr-vals options))))
                            ^PutItemRequest (.build)))))

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
         response (.query ddb-client (-> (QueryRequest/builder)
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
                                         ^QueryRequest (.build)))
         items (.items response)]
     (vec (map map-attribute-values-to-vals items)))))

(defn query-items-with-expression
  "Hakee tietueita tiettyjen ehtojen perusteella. Funktion key-expr
  -argumentti seuraa expression-syntaksia.

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
  ([key-expr options]
   (query-items-with-expression key-expr options (:herate-table env)))
  ([key-expr options table]
   (let [response (.query ddb-client (-> (QueryRequest/builder)
                                         (.tableName table)
                                         (.keyConditionExpression key-expr)
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
                                         ^QueryRequest (.build)))
         items (.items response)]
     (vec (map map-attribute-values-to-vals items)))))

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
   (let [req (-> (UpdateItemRequest/builder)
                 (.tableName table)
                 (.key (map-vals-to-attribute-values key-conds))
                 (.updateExpression (:update-expr options))
                 (cond->
                   (:expr-attr-names options)
                   (.expressionAttributeNames (:expr-attr-names options))
                   (:expr-attr-vals options)
                   (.expressionAttributeValues
                     (map-vals-to-attribute-values (:expr-attr-vals options)))
                   (:cond-expr options)
                   (.conditionExpression (:cond-expr options)))
                 ^UpdateItemRequest (.build))]
     (.updateItem ddb-client req))))

(defn get-item
  "Hakee yhden tietueen tietokannasta key-condsin perusteella. Avaimien syntaksi
  seuraa tätä mallia:
    {:<key> [:<tyyppi> <arvo>]}"
  ([key-conds]
   (get-item key-conds (:herate-table env)))
  ([key-conds table]
   (let [req (-> (GetItemRequest/builder)
                 (.tableName table)
                 (.key (map-vals-to-attribute-values key-conds))
                 ^GetItemRequest (.build))
         response (.getItem ddb-client req)
         item (.item response)]
     (map-attribute-values-to-vals item))))

(defn delete-item
  "Poistaa yhden tietueen tietokannasta key-condsin perusteella. Avaimien
  syntaksi seuraa tätä mallia:
    {:<key> [:<tyyppi> <arvo>]}"
  ([key-conds]
   (delete-item key-conds (:herate-table env)))
  ([key-conds table]
   (let [req (-> (DeleteItemRequest/builder)
                 (.tableName table)
                 (.key (map-vals-to-attribute-values key-conds))
                 ^DeleteItemRequest (.build))]
     (.deleteItem ddb-client req))))

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
  (let [req (-> (ScanRequest/builder)
                (cond->
                  (:filter-expression options)
                  (.filterExpression (:filter-expression options))
                  (:exclusive-start-key options)
                  (.exclusiveStartKey (:exclusive-start-key options))
                  (:expr-attr-names options)
                  (.expressionAttributeNames (:expr-attr-names options))
                  (:expr-attr-vals options)
                  (.expressionAttributeValues
                    (map-vals-to-attribute-values (:expr-attr-vals options))))
                (.tableName table)
                ^ScanRequest (.build))
        response (.scan ddb-client req)
        items (.items response)]
    {:items (into [] (map map-attribute-values-to-vals items))
     :last-evaluated-key (when (.hasLastEvaluatedKey response)
                           (.lastEvaluatedKey response))}))
