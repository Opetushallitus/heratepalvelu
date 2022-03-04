(ns oph.heratepalvelu.tep.archiveNippuTable
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.db.dynamodb :as ddb]))

(gen-class
  :name "oph.heratepalvelu.tep.archiveNippuTable"
  :methods [[^:static archiveNippuTable
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn do-query
  "Käy nipputaulun läpi jossakin järjestyksessä ja hakee nippuja, jotka kuuluvat
  annettuun kauteen. Jos last-evaluated-key ei ole nil, aloittaa scanin siitä
  paikasta scan-järjestyksessä."
  [kausi-start kausi-end last-evaluated-key]
  (let [options {:filter-expression "niputuspvm BETWEEN :start AND :end"
                 :expr-attr-vals    {":start" [:s kausi-start]
                                     ":end"   [:s kausi-end]}}
        options (if last-evaluated-key
                  (assoc options :exclusive-start-key last-evaluated-key)
                  options)]
    (ddb/scan options (:nippu-table env))))

(defn do-nippu-table-archiving
  "Arkistoi yhden kauden niput."
  [kausi-start kausi-end to-table]
  (loop [resp (do-query kausi-start kausi-end nil)]
    (doseq [item (:items resp)]
      (try
        (ddb/put-item (ddb/map-raw-vals-to-typed-vals item) {} to-table)
        (ddb/delete-item
          {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto item)]
           :niputuspvm                  [:s (:niputuspvm item)]}
          (:nippu-table env))
        (catch Exception e
          (log/error "Nipun arkistointi epäonnistui:"
                     (:ohjaaja_ytunnus_kj_tutkinto item)
                     (:niputuspvm item)
                     e))))
    (when (:last-evaluated-key resp)
      (recur (do-query kausi-start kausi-end (:last-evaluated-key resp))))))

(defn -archiveNippuTable
  "Arkistoi kaikki vanhoihin kausiin kuuluvat niput. Jos funktion sallittu
  ajoaika loppuu kesken ajon, voi joutua ajamaan funktion monta kertaa."
  [this event context]
  (do-nippu-table-archiving "2021-07-01"
                            "2022-06-30"
                            (:archive-table-2021-2022 env)))
