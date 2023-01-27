(ns oph.heratepalvelu.util.ONRDLQEventSourceMappingEnabledToggler
  "Käynnistää ja sammuttaa ONR DLQ käsittelyn muuttamalla Lambdan triggerin enabled-arvoa."
  (:require [aws-sdk.cloudformation :as cloudformation]))

(gen-class
  :name "oph.heratepalvelu.util.ONRDLQEventSourceMappingEnabledToggler"
  :methods [[^:static handleONRDLQToggler
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn -handleONRDLQToggler [event context]
  (let [cfn (cloudformation/client!)
        mapping-arn (:mapping-arn env)
        payload (:payload event)
        enable-state (:enabled payload)]
    (do
      (println mapping-arn)
      (println payload)
      (println enable-state)
      (cloudformation/update-event-source-mapping cfn {:event-source-mapping-arn mapping-arn :enabled enable-state}))))