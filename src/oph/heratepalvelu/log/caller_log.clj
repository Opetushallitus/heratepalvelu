(ns oph.heratepalvelu.log.caller-log
  (:require [clojure.tools.logging :as log]))

(defn- get-caller-header
  "Hakee kutsuvan järjestelmän oid:n Caller-Id-headerista, palauttaa
  no-caller-id-found jos headeria ei löydy"
  [headers]
  (get headers "Caller-Id" "no-caller-id-found"))

(defn- parse-schedule-rules
  "Parsii scheduled-eventistä kutsuvan säännön"
  [event]
  (.getResources event))

(defn log-caller-details-sqs
  "Logittaa SQS:n kutsuman lambdan nimen ja request-id. Parametri context on
  se context-parametri, jolla Lambdaa kutsutaan."
  [lambda-name context]
  (let [request-id (.getAwsRequestId context)]
    (log/info (str "Lambdaa " lambda-name
                   " kutsuttiin SQS:n toimesta (RequestId: " request-id " )"))))

(defn log-caller-details-scheduled
  "Logittaa scheduled lambdan nimen ja request-id. Parametrit context ja event
  ovat ne context- ja event-parametrit, joilla Lambdaa kutsutaan."
  [lambda-name event context]
  (let [request-id (.getAwsRequestId context)
        rules (parse-schedule-rules event)]
    (log/info (str "Lambdaa " lambda-name
                   " kutsuttiin ajastetusti säännöillä " rules
                   " (RequestId: " request-id " )"))))
