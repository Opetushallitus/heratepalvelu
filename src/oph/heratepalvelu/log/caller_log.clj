(ns oph.heratepalvelu.log.caller-log
  (:require [clojure.tools.logging :as log]))

(defn- get-caller-header
  "Hakee kutsuvan järjestelmän oid:n Caller-Id-headerista, palauttaa
  no-caller-id-found jos headeria ei löydy"
  [headers]
  (get headers "Caller-Id" "no-caller-id-found"))

(defn- get-sqs-event-messages
  "Parsii SQS-eventistä viestien bodyt listaksi"
  [event]
  (let [messages (seq (.getRecords event))]
    (list
      (map #(.getBody %) messages))))

(defn- parse-schedule-rules
  "Parsii scheduled-eventistä kutsuvan säännön"
  [event]
  (.getResources event))

(defn log-caller-details [name event context]
  (cond
    (= name "handleHOKSherate")
      (let [request-id (.getAwsRequestId context)
            body (get-sqs-event-messages event)]
        (log/info (str "Lambdaa " name
                       " kutsuttiin syötteellä " body
                       " (RequestId: " request-id " )")))
    (or
      (= name "handleSendEmails")
      (= name "handleUpdatedOpiskeluoikeus"))
        (let [request-id (.getAwsRequestId context)
              rules (parse-schedule-rules event)]
          (log/info (str "Lambdaa " name
                         " kutsuttiin ajastetusti säännöillä " rules
                         " (RequestId: " request-id " )")))))
