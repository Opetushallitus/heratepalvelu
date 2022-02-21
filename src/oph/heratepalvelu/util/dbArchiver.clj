(ns oph.heratepalvelu.util.dbArchiver
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.db.dynamodb :as ddb]))

(gen-class
  :name "oph.heratepalvelu.util.dbArchiver"
  :methods [[^:static handleDBArchiving
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn doArchiving [kausi to-table]
  (loop [resp (ddb/scan {:filter-expression "rahoituskausi = :kausi"
                         :expr-attr-vals    {":kausi" [:s kausi]}}
                        (:from-table env))]
    (doseq [item (:items resp)]
      (try
        (ddb/put-item (reduce
                        #(assoc %1
                                (first %2)
                                (cond (or (= (type (second %2))
                                             java.lang.Long)
                                          (= (type (second %2))
                                             java.lang.Integer))
                                      [:n (second %2)]
                                      (= (type (second %2)) java.lang.Boolean)
                                      [:bool (second %2)]
                                      :else
                                      [:s (second %2)]))
                        {}
                        (seq item))
                      {}
                      to-table)
        (ddb/delete-item {:toimija_oppija [:s (:toimija_oppija item)]
                          :tyyppi_kausi   [:s (:tyyppi_kausi item)]}
                         (:from-table env))
        (catch Exception e
          (log/error "Linkin arkistointi epäonnistui:"
                     (:kyselylinkki item)
                     e))))
    (when (:last-evaluated-key resp)
      (recur (ddb/scan {:filter-expression   "rahoituskausi = :kausi"
                        :expr-attr-vals      {":kausi" [:s kausi]}
                        :exclusive-start-key (:last-evaluated-key resp)}
                       (:from-table env))))))

(defn -handleDBArchiving [this event context]
  (doArchiving "2019-2020" (:to-table env))
  (doArchiving "2020-2021" (:to-table-2020-2021 env)))
