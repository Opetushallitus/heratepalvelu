(ns oph.heratepalvelu.tep.ehoksTimedOperationsHandler
  "Käsittelee ajastettuja operaatioita TEP-puolella."
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.ehoks :as ehoks]))

(gen-class
  :name "oph.heratepalvelu.tep.ehoksTimedOperationsHandler"
  :methods [[^:static handleTimedOperations
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn -handleTimedOperations
  "Pyytää ehoksia lähettää käsittelemättömät jaksot SQS:iin ja käynnistää
   työpaikkaohjaajan yhteystietojen poiston."
  [_ _ _]
  (log/info "Käynnistetään jaksojen lähetys")
  (let [resp (ehoks/get-paattyneet-tyoelamajaksot "2021-07-01"
                                                  (str (c/local-date-now))
                                                  1500)]
    (log/info "Lähetetty" (:data (:body resp)) "viestiä"))

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
