(ns oph.heratepalvelu.util.dbArchiver
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.db.dynamodb :as ddb])
  (:import (software.amazon.awssdk.services.dynamodb.model AttributeValue
                                                           ScanRequest)))

(gen-class
  :name "oph.heratepalvelu.util.dbArchiver"
  :methods [[^:static handleDBArchiving
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
                (.tableName (:from-table env))
                (.build))
        response (.scan ddb/ddb-client req)]
    (log/info (count (.items response)))
    response))

(def kausi "2019-2020")

(defn -handleDBArchiving [this event context]
  (loop [resp (scan {:filter-expression "rahoituskausi = :kausi"
                     :expr-attr-vals    {":kausi" (.build
                                                    (.s
                                                      (AttributeValue/builder)
                                                      kausi))}})]
    (doseq [item (map ddb/map-attribute-values-to-vals (.items resp))]
      (try
        (ddb/put-item {:toimija_oppija         [:s (:toimija_oppija item)]
                       :tyyppi_kausi           [:s (:tyyppi_kausi item)]
                       :kyselylinkki           [:s (:kyselylinkki item)]
                       :sahkoposti             [:s (:sahkoposti item)]
                       :suorituskieli          [:s (:suorituskieli item)]
                       :lahetystila            [:s (:lahetystila item)]
                       :lahetyspvm             [:s (:lahetyspvm item)]
                       :alkupvm                [:s (:alkupvm item)]
                       :heratepvm              [:s (:heratepvm item)]
                       :request-id             [:s (:request-id item)]
                       :oppilaitos             [:s (:oppilaitos item)]
                       :ehoks-id               [:n (:ehoks-id item)]
                       :opiskeluoikeus-id      [:s (:opiskeluoikeus-id item)]
                       :oppija-oid             [:s (:oppija-id item)]
                       :koulutustoimija        [:s (:koulutustoimija item)]
                       :kyselytyyppi           [:s (:kyselytyyppi item)]
                       :rahoituskausi          [:s (:rahoituskausi item)]
                       :viestintapalvelu-id    [:n (:viestintapalvelu-id item)]
                       :voimassa-loppupvm      [:s (:voimassa-loppupvm item)]
                       :tallennuspvm           [:s (:tallennuspvm item)]
                       :1.-muistutus-lahetetty [:s (:1.-muistutus-lahetetty
                                                     item)]

                       :2.-muistutus-lahetetty [:s (:2.-muistutus-lahetetty
                                                     item)]
                       :muistutukset           [:n (:muistutukset item)]
                       :check-suoritus         [:bool (:check-suoritus item)]}
                      {}
                      (:to-table env))
        (ddb/delete-item {:toimija_oppija [:s (:toimija_oppija item)]
                          :tyyppi_kausi   [:s (:tyyppi_kausi item)]})
        (catch Exception e
          (log/error "Linkin arkistointi epäonnistui:"
                     (:kyselylinkki item)
                     e))))
    (when (.hasLastEvaluatedKey resp)
      (recur (scan {:filter-expression   "rahoituskausi = :kausi"
                    :expr-attr-vals      {":kausi" (.build
                                                     (.s
                                                       (AttributeValue/builder)
                                                       kausi))}
                    :exclusive-start-key (.lastEvaluatedKey resp)})))))
