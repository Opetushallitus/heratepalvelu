(ns oph.heratepalvelu.tep.archiveJaksoTable
  "Käsittelee TEPin jaksotaulun arkistointia."
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.db.dynamodb :as ddb]))

(gen-class
  :name "oph.heratepalvelu.tep.archiveJaksoTable"
  :methods [[^:static archiveJaksoTable
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn do-query
  "Käy jaksotunnustaulun läpi jossakin järjestyksessä ja hakee jaksoja, jotka
  kuuluvat annettuun rahoituskauteen. Jos last-evaluated-key ei ole nil,
  aloittaa scanin siitä paikasta scan-järjestyksessä."
  [kausi last-evaluated-key]
  (let [options {:filter-expression "rahoituskausi = :kausi"
                 :expr-attr-vals    {":kausi" [:s kausi]}}
        options (if last-evaluated-key
                  (assoc options :exclusive-start-key last-evaluated-key)
                  options)]
    (ddb/scan options (:jaksotunnus-table env))))

(defn do-jakso-table-archiving
  "Arkistoi yhden rahoituskauden jaksot."
  [kausi to-table]
  (loop [resp (do-query kausi nil)]
    (doseq [item (:items resp)]
      (try
        (ddb/put-item (ddb/map-raw-vals-to-typed-vals item) {} to-table)
        (ddb/delete-item {:hankkimistapa_id [:n (:hankkimistapa_id item)]}
                         (:jaksotunnus-table env))
        (catch Exception e
          (log/error "Jakson arkistointi epäonnistui:"
                     (:hankkimistapa_id item)
                     e))))
    (when (:last-evaluated-key resp)
      (recur (do-query kausi (:last-evaluated-key resp))))))

(defn -archiveJaksoTable
  "Arkistoi kaikki vanhoihin kausiin kuuluvat jaksot. Jos funktion sallittu
  ajoaika loppuu kesken ajon, voi joutua ajamaan funktion monta kertaa."
  [_ _ _]
  (do-jakso-table-archiving "2021-2022" (:archive-table-2021-2022 env)))
