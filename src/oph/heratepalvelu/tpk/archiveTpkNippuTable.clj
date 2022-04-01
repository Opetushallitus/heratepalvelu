(ns oph.heratepalvelu.tpk.archiveTpkNippuTable
  "Käsittelee TPK:n nipputaulun arkistointia."
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.db.dynamodb :as ddb]))

(gen-class
  :name "oph.heratepalvelu.tpk.archiveTpkNippuTable"
  :methods [[^:static archiveTpkNippuTable
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn do-query
  "Käy TPK-nipputaulun läpi jossakin järjestyksessä ja hakee nippuja, jotka
  kuuluvat annettuun tiedokeruukauteen. Jos last-evaluated-key ei ole nil,
  aloittaa scanin siitä paikasta scan-järjestyksessä."
  [kausi-alkupvm last-evaluated-key]
  (let [options {:filter-expression "#kausi = :kausi"
                 :expr-attr-names   {"#kausi" "tiedonkeruu-alkupvm"}
                 :expr-attr-vals    {":kausi" [:s kausi-alkupvm]}}
        options (if last-evaluated-key
                  (assoc options :exclusive-start-key last-evaluated-key)
                  options)]
    (ddb/scan options (:tpk-nippu-table env))))

(defn do-tpk-nippu-table-archiving
  "Arkistoi yhden tiedonkeruukauden niput."
  [kausi-alkupvm to-table]
  (loop [resp (do-query kausi-alkupvm nil)]
    (doseq [item (:items resp)]
      (try
        (ddb/put-item (ddb/map-raw-vals-to-typed-vals item) {} to-table)
        (ddb/delete-item {:nippu-id [:s (:nippu-id item)]}
                         (:tpk-nippu-table env))
        (catch Exception e
          (log/error "TPK-nipun arkistointi epäonnistui:" (:nippu-id item) e))))
    (when (:last-evaluated-key resp)
      (recur (do-query kausi-alkupvm (:last-evaluated-key resp))))))

(defn -archiveTpkNippuTable
  "Arkistoi kaikki vanhoihin tiedonkeruukausiin kuuluvat niput. Jos funktion
  sallittu ajoaika loppuu kesken ajon, voi joutua ajamaan funktion monta
  kertaa."
  [this event context]
  (do-tpk-nippu-table-archiving "2021-07-01" (:archive-table-2021-fall env)))
