(ns oph.heratepalvelu.util.dbArchiver
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.db.dynamodb :as ddb]))

(gen-class
  :name "oph.heratepalvelu.util.dbArchiver"
  :methods [[^:static handleDBArchiving
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

;; TODO tämä täytyy muokata näin, että kentän nimi on mahdollista muuttaa
(defn doArchiving
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

(defn -handleDBArchiving
  "Tekee arkistointia niille rahoituskausille, jotka ovat jo päättyneet."
  [this event context]
  (doArchiving "2019-2020" (:to-table env))
  (doArchiving "2020-2021" (:to-table-2020-2021 env))
  (doArchiving "2021-2022" (:to-table-2021-2022 env)))
