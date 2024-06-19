(ns oph.heratepalvelu.tep.ehoksTimedOperationsHandler
  "Käsittelee ajastettuja operaatioita TEP-puolella."
  (:require [clojure.tools.logging :as log]
            [oph.heratepalvelu.common :as c]
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
  (let [resp (ehoks/get-paattyneet-tyoelamajaksot
               (str (c/current-rahoituskausi-alkupvm))
               (str (c/local-date-now))
               1500)]
    (log/info "Lähetetty" (:data (:body resp)) "viestiä")))
