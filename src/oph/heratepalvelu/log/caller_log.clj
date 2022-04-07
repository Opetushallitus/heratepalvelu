(ns oph.heratepalvelu.log.caller-log
  "Eräät räätälöidyt logitusfunktiot."
  (:require [clojure.tools.logging :as log])
  (:import (com.amazonaws.services.lambda.runtime Context)
           (com.amazonaws.services.lambda.runtime.events ScheduledEvent)))

(defn- parse-schedule-rules
  "Parsii scheduled-eventistä kutsuvan säännön"
  [^ScheduledEvent event]
  (.getResources event))

(defn log-caller-details-sqs
  "Logittaa SQS:n kutsuman lambdan nimen ja request-id. Parametri context on
  se context-parametri, jolla Lambdaa kutsutaan."
  [lambda-name ^Context context]
  (let [request-id (.getAwsRequestId context)]
    (log/info (str "Lambdaa " lambda-name
                   " kutsuttiin SQS:n toimesta (RequestId: " request-id " )"))))

(defn log-caller-details-scheduled
  "Logittaa scheduled lambdan nimen ja request-id. Parametrit context ja event
  ovat ne context- ja event-parametrit, joilla Lambdaa kutsutaan."
  [lambda-name event ^Context context]
  (let [request-id (.getAwsRequestId context)
        rules (parse-schedule-rules event)]
    (log/info (str "Lambdaa " lambda-name
                   " kutsuttiin ajastetusti säännöillä " rules
                   " (RequestId: " request-id " )"))))
