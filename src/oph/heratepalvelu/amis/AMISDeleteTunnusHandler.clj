(ns oph.heratepalvelu.amis.AMISDeleteTunnusHandler
  "Ottaa viestejä vastaan AmisDeleteTunnusQueuesta ja hoitaa tunnuksen poiston."
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.amis.AMISCommon :as ac]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.log.caller-log :refer [log-caller-details-sqs]]
            [schema.core :as s])
  (:import (com.amazonaws.services.lambda.runtime.events SQSEvent
                                                         SQSEvent$SQSMessage)
           (com.fasterxml.jackson.core JsonParseException)
           (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.amis.AMISDeleteTunnusHandler"
  :methods [[^:static handleDeleteTunnus
             [com.amazonaws.services.lambda.runtime.events.SQSEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(s/defschema delete-tunnus-schema
  "Tunnuksen poistoherätteen schema."
  {:kyselylinkki (s/constrained s/Str not-empty)})

(def delete-tunnus-checker
  "Tunnuksen poistoherätteen scheman tarkistusfunktio."
  (s/checker delete-tunnus-schema))

(defn delete-one-item
  "Poistaa yhden tietueen tietokannasta, jos item on olemassa."
  [item]
  (when item
    (try
      (ddb/delete-item {:toimija_oppija [:s (:toimija_oppija item)]
                        :tyyppi_kausi   [:s (:tyyppi_kausi item)]})
      (catch AwsServiceException e
        (log/error "Poistovirhe" (:kyselylinkki item) ":" e)
        (throw e)))))

(defn -handleDeleteTunnus
  "Käsittelee poistettavan tunnuksen ja poistaa sen tietokannasta."
  [_ ^SQSEvent event context]
  (log-caller-details-sqs "handleDeleteTunnus" context)
  (let [messages (seq (.getRecords event))]
    (doseq [^SQSEvent$SQSMessage msg messages]
      (try
        (let [herate (parse-string (.getBody msg) true)
              tunnus-checked (delete-tunnus-checker herate)]
          (if (some? tunnus-checked)
            (log/error {:herate herate :msg tunnus-checked})
            (delete-one-item
              (ac/get-herate-by-kyselylinkki! (:kyselylinkki herate)))))
        (catch JsonParseException e
          (log/error "Virhe viestin lukemisessa: " msg "\n" e))))))
