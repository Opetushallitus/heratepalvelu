(ns oph.heratepalvelu.tep.ehoksTimedOperationsHandler
  (:require [clojure.tools.logging :as log]
            [oph.heratepalvelu.external.ehoks :as ehoks]
            [clj-time.core :as t]))

(gen-class
  :name "oph.heratepalvelu.tep.ehoksTimedOperationsHandler"
  :methods [[^:static handleTimedOperations
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn -handleTimedOperations [this event context]
  (log/info "Käynnistetään jaksojen lähetys")
  (let [resp (ehoks/get-paattyneet-tyoelamajaksot (str (t/today)) "2021-04-01" 100)]
    (log/info "Lähetetty " (:data (:body resp)) " viestiä")))
