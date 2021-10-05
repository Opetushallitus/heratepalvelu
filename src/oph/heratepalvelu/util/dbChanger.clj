(ns oph.heratepalvelu.util.dbChanger
  (:require [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.ehoks :as ehoks]
            [oph.heratepalvelu.external.koski :as k]
            [environ.core :refer [env]]
            [clj-time.core :as t]
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
  (loop [resp (scan {:filter-expression "attribute_not_exists(oppisopimuksen_perusta)"})]
    (doseq [item (map ddb/map-attribute-values-to-vals (.items resp))]
      (try
        (let [oht (ehoks/get-osaamisen-hankkimistapa-by-id (:hankkimistapa_id item))]
          (ddb/update-item
            {:hankkimistapa_id [:s (:hankkimistapa_id item)]}
            {:update-expr "SET #value1 = :value1"
             :expr-attr-names {"#value1" "oppisopimuksen_perusta"}
             :expr-attr-vals {":value1" [:s (:oppisopimuksen-perusta oht)]}}
            (:table env)))
        (catch Exception e (do))))
    (when (.hasLastEvaluatedKey resp)
      (recur (scan {:exclusive-start-key (.lastEvaluatedKey resp)
                    :filter-expression "attribute_not_exists(oppisopimuksen_perusta)"})))))
