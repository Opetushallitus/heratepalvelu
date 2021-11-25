(ns oph.heratepalvelu.util.dbChanger
  (:require [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.external.ehoks :as ehoks]
            [oph.heratepalvelu.external.koski :as k]
            [environ.core :refer [env]]
            [clj-time.core :as t]
            [clojure.string :as s]
            [clojure.tools.logging :as log])
  (:import (software.amazon.awssdk.services.dynamodb.model ScanRequest AttributeValue)))

(gen-class
  :name "oph.heratepalvelu.util.dbChanger"
  :methods [[^:static handleDBUpdate
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]
            [^:static handleDBMarkIncorrectSuoritustyypit
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]
            [^:static handleDBGetPuuttuvatOppisopimuksenPerustat
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]
            [^:static handleAddTyopaikanNormalisoidutNimet
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]
            [^:static handleEH1269
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn scan [options]
  (let [req (-> (ScanRequest/builder)
                (cond->
                  (:filter-expression options)
                  (.filterExpression (:filter-expression options))
                  (:exclusive-start-key options)
                  (.exclusiveStartKey (:exclusive-start-key options))
                  (:expr-attr-vals options)
                  (.expressionAttributeValues (:expr-attr-vals options)))
                (.tableName (:table env))
                (.build))
        response (.scan ddb/ddb-client req)]
    (log/info (count (.items response)))
    response))

(defn -handleDBUpdate [this event context]
  (loop [resp (scan
                {:filter-expression "sms_kasittelytila = :value1"
                 :expr-attr-vals    {":value1" (.build (.s (AttributeValue/builder) "phone-mismatch"))}})]
    (doseq [item (map ddb/map-attribute-values-to-vals (.items resp))]
      (ddb/update-item
        {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto item)]
         :niputuspvm                  [:s (:niputuspvm item)]}
        {:update-expr     "SET #value1 = :value1"
         :expr-attr-names {"#value1" "sms_kasittelytila"}
         :expr-attr-vals {":value1" [:s "ei_lahetetty"]}}
        (:table env)))
    (when (.hasLastEvaluatedKey resp)
      (recur (scan
               {:filter-expression "sms_kasittelytila = :value1"
                :expr-attr-vals {":value1" (.build (.s (AttributeValue/builder) "phone-mismatch"))}
                :exclusive-start-key (.lastEvaluatedKey resp)})))))

(defn -handleDBMarkIncorrectSuoritustyypit [this event context]
  (loop [resp (scan {:filter-expression "kyselytyyppi = :value1"
                     :expr-attr-vals {":value1" (.build (.s (AttributeValue/builder) "tutkinnon_suorittaneet"))}})]
    (doseq [item (map ddb/map-attribute-values-to-vals (.items resp))]
      (try
        (if (not= (:check-suoritus item) nil)
          (do) ;; Arvo on jo olemassa; ei tarvitse lisätä mitään
          (let [opiskeluoikeus (k/get-opiskeluoikeus (:opiskeluoikeus-oid item))
                suoritus-koodi (:koodiarvo (:tyyppi (c/get-suoritus opiskeluoikeus)))
                db-suoritus-koodi (:kyselytyyppi item)
                mismatch (and (= suoritus-koodi "ammatillinentutkintoosittainen")
                              (= db-suoritus-koodi "tutkinnon_suorittaneet"))]
            (ddb/update-item
              {:toimija_oppija [:s (:toimija_oppija item)]
               :tyyppi_kausi [:s (:tyyppi_kausi item)]}
              {:update-expr "SET #value1 = :value1"
               :expr-attr-names {"#value1" "check-suoritus"}
               :expr-attr-vals {":value1" [:bool mismatch]}}
              (:table env))))
        (catch Exception e (do))))
    (when (.hasLastEvaluatedKey resp)
      (recur (scan {:exclusive-start-key (.lastEvaluatedKey resp)
                    :filter-expression "kyselytyyppi = :value1"
                    :expr-attr-vals {":value1" (.build (.s (AttributeValue/builder) "tutkinnon_suorittaneet"))}})))))

(defn -handleDBGetPuuttuvatOppisopimuksenPerustat [this event context]
  (let [tag (first (.getResources event))]
    (loop [resp (scan {:filter-expression (str "attribute_not_exists(oppisopimuksen_perusta) "
                                               "AND hankkimistapa_tyyppi = :htp "
                                               "AND jakso_loppupvm >= :pvm "
                                               "AND dbchangerin_kasittelema <> :dbc")
                       :expr-attr-vals {":htp" (.build (.s (AttributeValue/builder) "oppisopimus"))
                                        ":pvm" (.build (.s (AttributeValue/builder) "2021-07-01"))
                                        ":dbc" (.build (.s (AttributeValue/builder) tag))}})]
      (doseq [item (map ddb/map-attribute-values-to-vals (.items resp))]
        (try
          (let [oht (ehoks/get-osaamisen-hankkimistapa-by-id (:hankkimistapa_id item))
                perusta (:oppisopimuksen-perusta-koodi-uri oht)]
            (if perusta
              (ddb/update-item
                {:hankkimistapa_id [:n (:hankkimistapa_id item)]}
                {:update-expr "SET #value1 = :value1, #dbc = :dbc"
                 :expr-attr-names {"#value1" "oppisopimuksen_perusta"
                                   "#dbc" "dbchangerin_kasittelema"}
                 :expr-attr-vals {":value1" [:s (last (s/split perusta #"_"))]
                                  ":dbc" [:s tag]}}
                (:table env))
              (ddb/update-item
                {:hankkimistapa_id [:n (:hankkimistapa_id item)]}
                {:update-expr "SET #dbc = :dbc"
                 :expr-attr-names {"#dbc" "dbchangerin_kasittelema"}
                 :expr-attr-vals {":dbc" [:s tag]}}
                (:table env))))
          (catch Exception e
            (log/error e))))
      (when (.hasLastEvaluatedKey resp)
        (recur (scan {:exclusive-start-key (.lastEvaluatedKey resp)
                      :filter-expression (str "attribute_not_exists(oppisopimuksen_perusta) "
                                              "AND hankkimistapa_tyyppi = :htp "
                                              "AND jakso_loppupvm >= :pvm "
                                              "AND dbchangerin_kasittelema <> :dbc")
                      :expr-attr-vals {":htp" (.build (.s (AttributeValue/builder) "oppisopimus"))
                                       ":pvm" (.build (.s (AttributeValue/builder) "2021-07-01"))
                                       ":dbc" (.build (.s (AttributeValue/builder) tag))}}))))))

(defn -handleAddTyopaikanNormalisoidutNimet [this event context]
  (loop [resp (scan {:filter-expression "attribute_not_exists(tyopaikan_normalisoitu_nimi)"})]
    (doseq [item (map ddb/map-attribute-values-to-vals (.items resp))]
      (try
        (let [normalisoitu-nimi (c/normalize-string (:tyopaikan_nimi item))]
          (ddb/update-item
            {:hankkimistapa_id [:n (:hankkimistapa_id item)]}
            {:update-expr "SET #value = :value"
             :expr-attr-names {"#value" "tyopaikan_normalisoitu_nimi"}
             :expr-attr-vals {":value" [:s normalisoitu-nimi]}}
            (:table env)))
        (catch Exception e
          (log/error e))))
    (when (.hasLastEvaluatedKey resp)
      (recur (scan {:exclusive-start-key (.lastEvaluatedKey resp)
                    :filter-expression "attribute_not_exists(tyopaikan_normalisoitu_nimi)"})))))

(defn- doEH1269scan
  ([] (doEH1269scan nil))
  ([lastEvaluatedKey]
   (let [options {:filter-expression
                  (str "heratepvm >= :alkupvm AND heratepvm <= :loppupvm "
                       "AND attribute_not_exists(EH1269updated)")
                  :expr-attr-vals
                  {":alkupvm" (.build
                                (.s (AttributeValue/builder) "2021-11-02"))
                   ":loppupvm" (.build
                                 (.s (AttributeValue/builder) "2021-11-18"))}}]
     (scan (if lastEvaluatedKey
             (assoc options :exclusive-start-key lastEvaluatedKey)
             options)))))

(defn -handleEH1269 [this event context]
  (loop [resp (doEH1269scan)]
    (doseq [item (map ddb/map-attribute-values-to-vals (.items resp))]
      (try
        (let [ht (arvo/get-hankintakoulutuksen-toteuttaja (:ehoks-id item))
              toimipiste-oid (arvo/get-toimipiste
                               (c/get-suoritus
                                 (k/get-opiskeluoikeus-catch-404
                                   (:opiskeluoikeus-oid item))))]
          (ddb/update-item
            {:toimija_oppija [:s (:toimija_oppija item)]
             :tyyppi_kausi [:s (:tyyppi_kausi item)]}
            {:update-expr "SET #ht = :ht, #tpo = :tpo, #update = :updated"
             :expr-attr-names {"#ht" "hankintakoulutuksen-toteuttaja"
                               "#tpo" "toimipiste-oid"
                               "#updated" "EH1269updated"}
             :expr-attr-vals {":ht" [:s (str ht)]
                              ":tpo" [:s (str toimipiste-oid)]
                              ":updated" [:bool true]}}
            (:table env)))
       (catch Exception e
         (log/error e))))
    (when (.hasLastEvaluatedKey resp)
      (recur (doEH1269scan (.lastEvaluatedKey resp))))))
