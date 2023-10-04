(ns oph.heratepalvelu.tep.kestojenUudelleenlaskentaHandler
  "TODO"
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.log.caller-log :refer
             [log-caller-details-scheduled]]
            [oph.heratepalvelu.tep.niputusHandler :as nh]
            [oph.heratepalvelu.tep.tepCommon :as tc]))

(gen-class :name "oph.heratepalvelu.tep.kestojenUudelleenlaskentaHandler"
           :methods
           [[^:static handleKestojenUudelleenlaskenta
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(def rahoituskausi {:alkupvm "2022-07-01" :loppupvm "2023-06-30"})

(defn scan-for-unhandled-nippus!
  [exclusive-start-key]
  (let [resp (ddb/scan
               {:filter-expression
                (str
                  "niputuspvm >= :rahoituskausi_alkupvm AND "
                  "niputuspvm <= :rahoituskausi_loppupvm AND "
                  "attribute_not_exists(keston_uudelleenlaskenta_suoritettu)")
                :exclusive-start-key exclusive-start-key
                :expr-attr-vals {":rahoituskausi_alkupvm"
                                 [:s (:alkupvm rahoituskausi)]
                                 ":rahoituskausi_loppupvm"
                                 [:s (:loppupvm rahoituskausi)]}}
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
     :filter-expression
     "attribute_exists(kesto) AND attribute_not_exists(kesto_vanha)"}
    (:jaksotunnus-table env)))

(defn -handleKestojenUudelleenlaskenta
  [_ event ^com.amazonaws.services.lambda.runtime.Context context]
  (log-caller-details-scheduled "handleKestojenUudelleenlaskenta" event context)
  (loop [niput (scan-for-unhandled-nippus! nil)]
    (doseq [nippu (:items niput)]
      (let [jaksot (query-jaksot-with-kesto! nippu)
            kestot (nh/jaksojen-kestot! jaksot)]
        (log/info "Jaksot: " jaksot ", Kestot: " kestot)
        (doseq [jakso jaksot]
          (tc/update-jakso jakso
                           {:kesto [:n (get kestot
                                            (:hankkimistapa_id jakso)
                                            0)]
                            :kesto_vanha [:n (:kesto jakso)]})))
      (tc/update-nippu nippu {:keston_uudelleenlaskenta_suoritettu
                              [:s (str (c/local-date-now))]}))
    (when (and (< 30000 (.getRemainingTimeInMillis context))
               (:last-evaluated-key niput))
      (recur (scan-for-unhandled-nippus! (:last-evaluated-key niput))))))
