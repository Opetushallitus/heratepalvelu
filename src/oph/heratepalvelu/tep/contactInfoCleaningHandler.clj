(ns oph.heratepalvelu.tep.contactInfoCleaningHandler
  "Käsittelee ajastettuja työpaikkaohjaajien yhteistietojen siivouksia."
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.ehoks :as ehoks]))

(gen-class
  :name "oph.heratepalvelu.tep.contactInfoCleaningHandler"
  :methods [[^:static cleanContactInfo
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn -cleanContactInfo
  "Käynnistää työpaikkaohjaajan yhteystietojen poiston."
  [_ _ _]
  (log/info "Käynnistetään työpaikkaohjaajan yhteystietojen poisto")
  (let [hankkimistavat
        (get-in (ehoks/delete-tyopaikkaohjaajan-yhteystiedot)
                [:body :data :hankkimistapa-ids])
        counter (atom 0)]
    (doseq [hankkimistapa_id hankkimistavat]
      (log/info "Käsitellään oht" hankkimistapa_id)
      (let [resp (ddb/scan {:filter-expression   "#id = :id"
                            :expr-attr-names     {"#id" "hankkimistapa_id"}
                            :expr-attr-vals      {":id" [:n hankkimistapa_id]}}
                           (:jaksotunnus-table env))
            items (:items resp)]
        (when (seq items)
          (log/info "Poistetaan ohjaajan yhteystiedot (hankkimistapa_id = "
                    hankkimistapa_id
                    ")")
          (ddb/update-item
            {:hankkimistapa_id [:n hankkimistapa_id]}
            {:update-expr "SET #eml = :eml_value, #puh = :puh_value"
             :expr-attr-names {"#eml" "ohjaaja_email"
                               "#puh" "ohjaaja_puhelinnumero"}
             :expr-attr-vals {":eml_value" [:s ""] ":puh_value" [:s ""]}}
            (:jaksotunnus-table env))
          (swap! counter inc))))
    (log/info "Poistettu" @counter "ohjaajan yhteystiedot")))
