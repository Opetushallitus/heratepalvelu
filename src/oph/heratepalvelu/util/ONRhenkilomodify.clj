(ns oph.heratepalvelu.util.ONRhenkilomodify
  "Käsittelee SQS:stä vastaanotettavia ONR:n henkilömuutoksia."
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.external.ehoks :as ehoks])
  (:import (com.amazonaws.services.lambda.runtime.events SQSEvent$SQSMessage)
           (clojure.lang ExceptionInfo)))

(gen-class
  :name "oph.heratepalvelu.util.ONRhenkilomodify"
  :methods [[^:static handleONRhenkilomodify
             [com.amazonaws.services.lambda.runtime.events.SQSEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn -handleONRhenkilomodify
  "Käsittelee ONR:n henkilömuutokset ja lähettää niistä tiedon Ehoksiin."
  [_ ^com.amazonaws.services.lambda.runtime.events.SQSEvent event context]
  (log-caller-details-sqs "ONRhenkilomodify" context)
  (let [messages (seq (.getRecords event))]
    (doseq [^SQSEvent$SQSMessage msg messages]
      (try
        (let [body (parse-string (.getBody msg) true)
              message (parse-string (:Message body) true)]
          (ehoks/post-henkilomodify-event (:oidHenkilo message)))
        (catch ExceptionInfo e
          (log/error e))))))
