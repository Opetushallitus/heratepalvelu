(ns oph.heratepalvelu.tep.kestojenUudelleenlaskentaHandler
  "TODO"
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.tep.tepCommon :as tc]
            [oph.heratepalvelu.tep.niputusHandler :as nh]
            [oph.heratepalvelu.log.caller-log :refer [log-caller-details-scheduled]]))

(gen-class :name "oph.heratepalvelu.tep.kestojenUudelleenlaskentaHandler"
           :methods
           [[^:static handleKestojenUudelleenlaskenta
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(def rahoituskauden-alkupvm "2022-07-01")

(defn scan-for-unhandled-nippus!
  [exclusive-start-key]
  (let [resp (ddb/scan
               {:filter-expression "niputuspvm >= :alkupvm AND attribute_not_exists(#suoritettu)"
                :exclusive-start-key exclusive-start-key
                :expr-attr-names {"#suoritettu" "kestojen-uudelleenlaskenta-suoritettu"}
                :expr-attr-vals {":alkupvm" [:s rahoituskauden-alkupvm]}}
                (:nippu-table env))]
    (log/info "Haettiin onnistuneesti " (count (:items resp)) " nippua")
    resp))

(defn query-jaksot-with-kesto!
  "Hakee tietokannasta ne jaksot, jotka kuuluvat parametrina annettuun nippuun.
  Suodattaan jaksoja edelleen siten, että jäljelle jääviltä jaksoilta löytyy
  vanhastaan laskettu kesto, mutta ei uudella tavalla laskettua kestoa."
  [nippu]
  (ddb/query-items
    {:ohjaaja_ytunnus_kj_tutkinto
     [:eq [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]]
     :niputuspvm [:eq [:s (:niputuspvm nippu)]]}
    {:index "niputusIndex"
     :filter-expression "attribute_exists(kesto) AND attribute_not_exists(#vanha)"
     :expr-attr-names {"#vanha" "kesto-vanha"}}
    (:jaksotunnus-table env)))

(defn calculate-kestot!
  [jaksot]
  (apply
    merge
    (map (fn [oppijan-jaksot]
           (let [concurrent-jaksot (nh/get-concurrent-jaksot-from-ehoks!
                                    oppijan-jaksot)
                 opiskeluoikeudet  (nh/get-and-memoize-opiskeluoikeudet!
                                    concurrent-jaksot)]
             (nh/calculate-kestot concurrent-jaksot opiskeluoikeudet)))
          ; Ryhmitellään jaksot oppija-oid:n perusteella:
         (vals (group-by :oppija_oid jaksot)))))

(defn -handleKestojenUudelleenlaskenta
  [_ event ^com.amazonaws.services.lambda.runtime.Context context]
  (log-caller-details-scheduled "handleKestojenUudelleenlaskenta" event context)
    (loop [niput (scan-for-unhandled-nippus! nil)]
      (doseq [nippu (:items niput)]
        (let [jaksot (query-jaksot-with-kesto! nippu)
              kestot (calculate-kestot! jaksot)]
          (log/info "Jaksot: " jaksot ", Kestot: " kestot)
          (doseq [jakso jaksot]
            (tc/update-jakso jakso
                             {:kesto [:n (get kestot
                                              (:hankkimistapa_id jakso)
                                              0)]
                              :kesto-vanha [:n (:kesto jakso)]})))
        (tc/update-nippu nippu {:kestojen-uudelleenlaskenta-suoritettu [:bool true]}))
      (when (and (< 30000 (.getRemainingTimeInMillis context))
                 (:last-evaluated-key niput))
        (recur (scan-for-unhandled-nippus! (:last-evaluated-key niput))))))
