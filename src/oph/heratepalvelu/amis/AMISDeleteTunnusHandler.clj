(ns oph.heratepalvelu.amis.AMISDeleteTunnusHandler
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.log.caller-log :refer :all]
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

(defn -handleDeleteTunnus [this event context]
  (log-caller-details-sqs "handleDeleteTunnus" context)
  (let [messages (seq (.getRecords event))]
    (doseq [msg messages]
      (try
        (let [herate (parse-string (.getBody msg))
              kyselylinkki (:kyselylinkki herate)]
          (if (some? (delete-tunnus-checker herate))
            (log/error {:herate herate :msg (delete-tunnus-checker herate)})
            (try
              (let [item (first (ddb/query-items
                                  {:kyselylinkki [:eq [:s kyselylinkki]]}
                                  {:index "resendIndex"}))
                    toimija-oppija (:toimija_oppija item)
                    tyyppi-kausi (:tyyppi_kausi item)]
                (when item
                  (ddb/delete-item {:toimija_oppija [:s toimija-oppija]
                                    :tyyppi_kausi   [:s tyyppi-kausi]})))
              (catch AwsServiceException e
                (log/error "Virhe kyselylinkin" kyselylinkki "poistossa" e)
                (throw e)))))
        (catch JsonParseException e
          (log/error "Virhe viestin lukemisessa: " msg "\n" e))))))
