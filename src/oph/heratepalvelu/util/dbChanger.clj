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
            [^:static handleDBFixErroneousEiNiputetaJaksot
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

(defn -handleDBFixErroneousEiNiputetaJaksot [this event context]
  (loop [resp (scan {:filter-expression "kasittelytila = :tila"
                     :expr-attr-vals {":tila" (.build (.s (AttributeValue/builder) "ei_niputeta"))}})]
    (doseq [item (map ddb/map-attribute-values-to-vals (.items resp))]
      (try
        (let [jaksot (ddb/query-items {:ohjaaja_ytunnus_kj_tutkinto [:eq [:s (:ohjaaja_ytunnus_kj_tutkinto item)]]
                                       :niputuspvm                  [:eq [:s (:niputuspvm item)]]}
                                      {:index "niputusIndex"
                                       :filter-expression "attribute_not_exists(tunnus)"}
                                      (:jaksotunnus-table env))
              jakso (first jaksot)
              opiskeluoikeus (k/get-opiskeluoikeus
                               (:opiskeluoikeus_oid jakso))
              suoritus (c/get-suoritus opiskeluoikeus)
              arvo-resp (arvo/create-jaksotunnus
                          (arvo/build-jaksotunnus-request-body
                            {:tyopaikan-nimi (:tyopaikan_nimi jakso)
                             :tyopaikan-ytunnus (:tyopaikan_ytunnus jakso)
                             :tutkinnonosa-nimi (:tutkinnonosa_nimi jakso)
                             :tutkinnonosa-koodi (:tutkinnonosa_koodi jakso)
                             :alkupvm (:jakso_alkupvm jakso)
                             :loppupvm (:jakso_loppupvm jakso)
                             :osa-aikaisuus (:osa_aikaisuus jakso)
                             :hankkimistapa-tyyppi (:hankkimistapa_tyyppi jakso)}
                            (:kesto jakso)
                            opiskeluoikeus
                            (:request_id jakso)
                            (:koulutustoimia jakso)
                            suoritus
                            (:alkupvm jakso)))
              tunnus (:tunnus (:body arvo-resp))]
          (ddb/update-item
            {:hankkimistapa_id [:n (:hankkimistapa_id jakso)]}
            {:update-expr "SET #tunnus = :tunnus"
             :expr-attr-names {"#tunnus" "tunnus"}
             :expr-attr-vals {":tunnus" [:s tunnus]}}
            (:jaksotunnus-table env))
          (ddb/update-item
            {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto item)]
             :niputuspvm                  [:s (:niputuspvm item)]}
            {:update-expr "SET #tila = :tila, #smstila = :smstila"
             :expr-attr-names {"#tila" "kasittelytila"
                               "#smstila" "sms_kasittelytila"}
             :expr-attr-vals {":tila" [:s (:ei-niputettu c/kasittelytilat)]
                              ":smstila" [:s (:ei-lahetetty c/kasittelytilat)]}}
            (:table env)))
        (catch Exception e
          (log/error e))))
    (when (.hasLastEvaluatedKey resp)
      (recur (scan {:exclusive-start-key (.lastEvaluatedKey resp)
                    :filter-expression "kasittelytila = :tila"
                    :expr-attr-vals {":tila" (.build (.s (AttributeValue/builder) "ei_niputeta"))}})))))
