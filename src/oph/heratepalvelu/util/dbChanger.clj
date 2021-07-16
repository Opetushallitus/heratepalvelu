(ns oph.heratepalvelu.util.dbChanger
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [environ.core :refer [env]]
            [clj-time.core :as t]))

(gen-class
  :name "oph.heratepalvelu.util.dbChanger"
  :methods [[^:static handleDBUpdate
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn -handleDBUpdate [this event context]
  (let [items (ddb/scan
                {:filter-expression "attribute_exists(tyopaikkaohjaaja_puhelinnumero)"}
                (:nippu-table env))]
    (doseq [item items]
      (ddb/update-item
        {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto item)]
         :niputuspvm                  [:s (:niputuspvm item)]}
        {:update-expr     "SET #value1 = :value1, #value2 = :value2 REMOVE tyopaikkaohjaaja_puhelinnumero"
         :expr-attr-names {"#value1" "sms_kasittelytila"
                           "#value2" "ohjaaja_puhelinnumero"}
         :expr-attr-vals {":value1" [:s "ei_lahetetty"]
                          ":value2" [:s (:tyopaikkaohjaaja_puhelinnumero item)]}}
        (:nippu-table env)))))
