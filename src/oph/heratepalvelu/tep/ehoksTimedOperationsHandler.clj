(ns oph.heratepalvelu.tep.ehoksTimedOperationsHandler
  (:require [clojure.tools.logging :as log]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.ehoks :as ehoks]))

(gen-class
  :name "oph.heratepalvelu.tep.ehoksTimedOperationsHandler"
  :methods [[^:static handleTimedOperations
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn -handleTimedOperations
  "Pyytää ehoksilta lähettää käsittelemättömät jaksot SQS:iin."
  [this event context]
  (log/info "Käynnistetään jaksojen lähetys")
  (let [resp (ehoks/get-paattyneet-tyoelamajaksot "2021-07-01"
                                                  (str (c/local-date-now))
                                                  100)]
    (log/info "Lähetetty" (:data (:body resp)) "viestiä")))
