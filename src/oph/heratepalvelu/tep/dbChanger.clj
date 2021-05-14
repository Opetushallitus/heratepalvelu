(ns oph.heratepalvelu.tep.dbChanger
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [environ.core :refer [env]]
            [clj-time.core :as t]))

(gen-class
  :name "oph.heratepalvelu.tep.dbChanger"
  :methods [[^:static handleDBUpdate
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn -handleDBUpdate [this event context]
  (let [items (ddb/query-items {:kasittelytila [:eq [:s "ei_lahetetty"]]
                                :niputuspvm    [:le [:s (str (t/today))]]}
                               {:index "niputusIndex"}
                               (:nippu-table env))]
    (doseq [item items]
      (ddb/update-item
        {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto item)]
         :niputuspvm                  [:s (:niputuspvm item)]}
        {:update-expr     "REMOVE kyselylinkki  SET #value = :value"
         :expr-attr-names {"#value" "kasittelytila"}
         :expr-attr-vals {":value" [:s "ei_niputettu"]}}
        (:nippu-table env)))))
