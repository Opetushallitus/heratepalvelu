(ns oph.heratepalvelu.amis.archiveHerateTable
  "Käsittelee AMISin herätetaulun arkistointia."
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.db.dynamodb :as ddb]))

(gen-class
  :name "oph.heratepalvelu.amis.archiveHerateTable"
  :methods [[^:static archiveHerateTable
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn do-herate-table-archiving
  "Arkistoi tietueet toiseen tauluun, jos niiden rahoituskaudet täsmäävät
  kausi-parametrin arvon kanssa. Arkistoidut tietueet poistetaan alkuperäisestä
  taulusta."
  [kausi to-table]
  (loop [resp (ddb/scan {:filter-expression "rahoituskausi = :kausi"
                         :expr-attr-vals    {":kausi" [:s kausi]}}
                        (:from-table env))]
    (doseq [item (:items resp)]
      (try
        (ddb/put-item (ddb/map-raw-vals-to-typed-vals item) {} to-table)
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

(defn -archiveHerateTable
  "Tekee arkistointia niille rahoituskausille, jotka ovat jo päättyneet."
  [this event context]
  (do-herate-table-archiving "2019-2020" (:to-table env))
  (do-herate-table-archiving "2020-2021" (:to-table-2020-2021 env))
  (do-herate-table-archiving "2021-2022" (:to-table-2021-2022 env)))
