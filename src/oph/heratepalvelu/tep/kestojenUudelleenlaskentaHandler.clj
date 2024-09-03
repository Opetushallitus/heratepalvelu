(ns oph.heratepalvelu.tep.kestojenUudelleenlaskentaHandler
  "Handler työpaikkajaksojen kestojen uudelleenlaskentaa varten."
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
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

(def alkupvm "2023-07-01")

(defn scan-for-jaksot-with-kesto!
  "Hakee tietokannasta ne jaksot, jotka ovat päättyneet `alkupvm` jälkeen ja
  joille on laskettu niputuksen yhteydessä kesto. Suodattaan jaksoja edelleen
  siten, että jäljelle jääviltä jaksoilta löytyy vanhastaan laskettu kesto,
  mutta ei uudella tavalla laskettua kestoa."
  [exclusive-start-key]
  (ddb/scan {:filter-expression   (str "jakso_loppupvm >= :alkupvm AND "
                                       "attribute_exists(kesto) AND "
                                       "attribute_not_exists(kesto_vanha)")
             :exclusive-start-key exclusive-start-key
             :expr-attr-vals      {":alkupvm" [:s alkupvm]}}
            (:jaksotunnus-table env)))

(defn -handleKestojenUudelleenlaskenta
  [_ event ^com.amazonaws.services.lambda.runtime.Context context]
  (log-caller-details-scheduled "handleKestojenUudelleenlaskenta" event context)
  (loop [resp (scan-for-jaksot-with-kesto! nil)]
    (let [jaksot (:items resp)
          kestot (nh/jaksojen-kestot! jaksot)]
      (doseq [jakso jaksot]
        (if-let [new-kesto (get kestot (nh/ids jakso))]
          (tc/update-jakso jakso
                           {:kesto       [:n new-kesto]
                            :kesto_vanha [:n (:kesto jakso)]})
          (log/warn "Couldn't calculate kesto for jakso with ids "
                    (nh/ids jakso)))))
    (when (and (< 30000 (.getRemainingTimeInMillis context))
               (:last-evaluated-key resp))
      (recur (scan-for-jaksot-with-kesto! (:last-evaluated-key resp))))))
