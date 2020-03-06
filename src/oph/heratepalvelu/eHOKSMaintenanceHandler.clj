(ns oph.heratepalvelu.eHOKSMaintenanceHandler
  (:require [oph.heratepalvelu.external.ehoks :as ehoks]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [clj-time.core :as t]))

(gen-class
  :name "oph.heratepalvelu.eHOKSMaintenanceHandler"
  :methods [[^:static handleMaintenance
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn -handleMaintenance [this event context]
  (log-caller-details-with-rules "handleMaintenance" event context)
  (ehoks/start-tyoelamajaksot-process "1970-01-01" (str (t/today))))