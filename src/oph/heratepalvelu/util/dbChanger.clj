(ns oph.heratepalvelu.util.dbChanger
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [environ.core :refer [env]]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.log.caller-log :as cl]
            [oph.heratepalvelu.amis.AMISCommon :as ac]
            [oph.heratepalvelu.tep.niputusHandler :as nip])
  (:import (software.amazon.awssdk.services.dynamodb.model
             ScanRequest ScanResponse AttributeValue)
           (java.time LocalDate)))

(gen-class
  :name "oph.heratepalvelu.util.dbChanger"
  :methods [[^:static handleDBUpdateTep
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]
            [^:static updateSmsLahetystila
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn ^ScanResponse scan [options]
  (let [^ScanRequest
        req (-> (ScanRequest/builder)
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
    ;; FIXME: probably outdated? ; (log/info (count (.items response)))
    response))

(defn- attr [^String input] (.build (.s (AttributeValue/builder) input)))

(defn -handleDBUpdateTep [this event context]
  (loop [resp (scan {:filter-expression
                     (str "jakso_loppupvm >= :start "
                          "AND jakso_loppupvm <= :end "
                          "AND attribute_not_exists(uudelleenlaskettu_kesto)")
                     :expr-attr-vals {":start" (attr "2021-11-01")
                                      ":end" (attr "2021-12-31")}})]
    (doseq [item (map ddb/map-attribute-values-to-vals (.items resp))]
      (try
        (let [dbjakso (ddb/query-items
                        {:ohjaaja_ytunnus_kj_tutkinto
                         [:eq [:s (:ohjaaja_ytunnus_kj_tutkinto item)]]}
                        {:index "niputusIndex"}
                        (:table env))
              jakso (first dbjakso)
              uudelleenlaskettu_kesto (:uudelleenlaskettu_kesto jakso)]
          (when (nil? uudelleenlaskettu_kesto)
            (println (str "Lasketaan oppijan "
                          (:oppija_oid jakso)
                          " jaksojen kestot uudelleen niputuksessa "
                          (:niputuspvm jakso)))
            (let [oppijan-kaikki-jaksot
                  (ddb/query-items
                    {:oppija_oid [:eq [:s (:oppija_oid jakso)]]}
                    {:index "tepDbChangerIndex"
                     :filter-expression (str "jakso_loppupvm >= :start "
                                             "jakso_loppupvm <= :end"
                                             "AND attribute_exists(#tunnus)")
                     :expr-attr-names {"#tunnus" "tunnus"}
                     :expr-attr-vals {":start" (attr "2021-11-01")
                                      ":end" (attr "2021-12-31")}}
                    (:table env))
                  uudelleenlasketut-kestot
                  (nip/jaksojen-kestot! oppijan-kaikki-jaksot)]
              (println uudelleenlasketut-kestot)
              (doseq [jakso oppijan-kaikki-jaksot]
                (println "Päivitetään jaksolle"
                         (:hankkimistapa_id jakso)
                         "uudelleenlaskettu_kesto.")
                (println "Vanha kesto"
                         (:kesto jakso)
                         "- Uudelleen laskettu kesto"
                         (get uudelleenlasketut-kestot
                              (:hankkimistapa_id jakso)))))))
        (catch Exception e (log/error e))))
    (when (.hasLastEvaluatedKey resp)
      (recur (scan
               {:filter-expression
                (str "jakso_loppupvm >= :start "
                     "AND jakso_loppupvm <= :end "
                     "AND attribute_not_exists(uudelleenlaskettu_kesto)")
                :expr-attr-vals {":start" (attr "2021-11-01")
                                 ":end" (attr "2021-12-31")}})))))

(defn queryWrongStatuses
  [limit]
  (ddb/query-items-with-expression
    "#smstila = :eilahetetty AND #alku <= :pvm"
    {:index "smsIndex"
     :filter-expression "#ltila = :eilaheteta OR #ltila = :eilahetetaoo"
     :expr-attr-names {"#smstila" "sms-lahetystila"
                       "#alku" "alkupvm"
                       "#ltila" "lahetystila"}
     :expr-attr-vals {":eilahetetty" [:s (:ei-lahetetty c/kasittelytilat)]
                      ":pvm" [:s (str (c/local-date-now))]
                      ":eilaheteta" [:s (:ei-laheteta c/kasittelytilat)]
                      ":eilahetetaoo"
                      [:s (:ei-laheteta-oo-ei-loydy c/kasittelytilat)]}
     :limit limit}
    (:herate-table env)))

(defn -updateSmsLahetystila
  [_ event ^com.amazonaws.services.lambda.runtime.Context context]
  (cl/log-caller-details-scheduled "AMISSMSHandler" event context)
  (loop [lahetettavat (queryWrongStatuses 20)]
    (log/info
      "Käsitellään" (count lahetettavat)
      "väärässä tilassa olevaa sms-lahetystilaa.")
    (when (seq lahetettavat)
      (doseq [herate lahetettavat]
        (try
          (ac/update-herate
            herate
            {:sms-lahetystila [:s (:ei-laheteta c/kasittelytilat)]})
          (catch Exception e
            (log/error "Virhe AMIS SMS-lähetystila päivityksessä kun"
                       "tila on väärä."
                       e))))
      (when (< 60000 (.getRemainingTimeInMillis context))
        (recur (queryWrongStatuses 20))))))
