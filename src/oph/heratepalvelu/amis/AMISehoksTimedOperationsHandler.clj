(ns oph.heratepalvelu.amis.AMISehoksTimedOperationsHandler
  (:require [clj-time.core :as t]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.external.ehoks :as ehoks]))

(gen-class
  :name "oph.heratepalvelu.amis.AMISehoksTimedOperationsHandler"
  :methods [[^:static handleAMISTimedOperations
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn -handleAMISTimedOperations [this event context]
  (log/info "Käynnistetään herätteiden lähetys")
  (let [resp (ehoks/get-retry-kyselylinkit "2021-07-01" (str (t/today)) 100)]
    (log/info "Lähetetty " (:data (:body resp)) " viestiä")))
