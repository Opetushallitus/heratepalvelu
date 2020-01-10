(ns oph.heratepalvelu.eHOKSMaintenanceHandler
  (:require [oph.heratepalvelu.external.ehoks :as ehoks]
            [oph.heratepalvelu.log.caller-log :refer :all]))

(gen-class
  :name "oph.heratepalvelu.eHOKSMaintenanceHandler"
  :methods [[^:static handleMaintenance
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn -handleMaintenance [this event context]
  (log-caller-details "handleMaintenance" event context)
  (ehoks/call-maintenance-endpoints))