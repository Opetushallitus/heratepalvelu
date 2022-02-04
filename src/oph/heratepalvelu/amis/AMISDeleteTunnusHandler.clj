(ns oph.heratepalvelu.amis.AMISDeleteTunnusHandler
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.log.caller-log :refer [log-caller-details-sqs]]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [schema.core :as s])
  (:import (com.fasterxml.jackson.core JsonParseException)
           (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.amis.AMISDeleteTunnusHandler"
  :methods [[^:static handleDeleteTunnus
             [com.amazonaws.services.lambda.runtime.events.SQSEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(s/defschema delete-tunnus-schema
  {:kyselylinkki (s/constrained s/Str not-empty)})

(def delete-tunnus-checker (s/checker delete-tunnus-schema))

(defn delete-one-item [item]
  (when item
    (try
      (ddb/delete-item {:toimija_oppija [:s (:toimija_oppija item)]
                        :tyyppi_kausi   [:s (:tyyppi_kausi item)]})
      (catch AwsServiceException e
        (log/error "Poistovirhe" (:kyselylinkki item) ":" e)
        (throw e)))))

(defn get-item-by-kyselylinkki [kyselylinkki]
  (try
    (first (ddb/query-items {:kyselylinkki [:eq [:s kyselylinkki]]}
                            {:index "resendIndex"}))
    (catch AwsServiceException e
      (log/error "Hakuvirhe" kyselylinkki ":" e)
      (throw e))))

(defn -handleDeleteTunnus [this event context]
  (log-caller-details-sqs "handleDeleteTunnus" context)
  (let [messages (seq (.getRecords event))]
    (doseq [msg messages]
      (try
        (let [herate (parse-string (.getBody msg) true)
              tunnus-checked (delete-tunnus-checker herate)]
          (if (some? tunnus-checked)
            (log/error {:herate herate :msg tunnus-checked})
            (delete-one-item
              (get-item-by-kyselylinkki (:kyselylinkki herate)))))
        (catch JsonParseException e
          (log/error "Virhe viestin lukemisessa: " msg "\n" e))))))
