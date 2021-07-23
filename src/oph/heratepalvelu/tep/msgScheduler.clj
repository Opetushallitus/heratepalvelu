(ns oph.heratepalvelu.tep.msgScheduler
  (:require [clojure.tools.logging :as log])
  (:import (java.util.concurrent Executors)))

(gen-class
  :name "oph.heratepalvelu.tep.msgScheduler"
  :methods [[^:static handleSendMessages
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn- process-email [])

(defn- process-sms [])

(defn- process-email-reminders [])

(defn- process-sms-reminders [])

(defn -handleSendMessages
  (let [pool (Executors/newFixedThreadPool 4)
        tasks [process-email process-sms process-email-reminders process-sms-reminders]]
    (doseq [f (.invokeAll pool tasks)]
      (log/info (.get f)))
    (.shutdown pool)))
