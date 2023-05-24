(ns oph.heratepalvelu.amis.AMISEmailResendHandler
  "Ottaa vastaan viestejä ehoksAmisResendQueuesta ja merkitsee kyseessä olevan
  kyselylinkin sähköpostin lähetettäväksi uudestaan, jos osoite löytyy."
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.amis.AMISCommon :as ac]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.log.caller-log :refer [log-caller-details-sqs]]
            [schema.core :as s])
  (:import (com.amazonaws.services.lambda.runtime.events SQSEvent
                                                         SQSEvent$SQSMessage)
           (com.fasterxml.jackson.core JsonParseException)
           (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.amis.AMISEmailResendHandler"
  :methods [[^:static handleEmailResend
             [com.amazonaws.services.lambda.runtime.events.SQSEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(s/defschema resend-schema
  "Uuudelleenlähetyksen herätteen schema."
  {:kyselylinkki (s/constrained s/Str not-empty)
   (s/optional-key :sahkoposti) (s/constrained s/Str not-empty)})

(def resend-schema-errors
  "Uudelleenlähetyksen herätteen scheman tarkistusfunktio."
  (s/checker resend-schema))

(defn handle-single-herate!
  "Käsittelee yhden pyynnön sähköpostin lähettämiseksi uudestaan."
  [herate]
  (log/info "Käsitellään resend-heräte" herate)
  (let [kyselylinkki (:kyselylinkki herate)
        schema-errors (resend-schema-errors herate)
        db-herate (ac/get-herate-by-kyselylinkki! kyselylinkki)
        sahkoposti (or (not-empty (:sahkoposti herate))
                       (:sahkoposti db-herate))]
    (cond (some? schema-errors)
          (log/error "Epämuodostunut heräte:" schema-errors)

          (not kyselylinkki)
          (log/error "eHOKS ei lähettänyt kyselylinkkiä herätteessä")

          (not db-herate)
          (log/error "Ei löytynyt herätettä kyselylinkillä" kyselylinkki)

          :else
          (do
            (when (empty? (:sahkoposti herate))
              (log/warn "Ei sähköpostia, käytetään dynamoon tallennettua"
                        sahkoposti))
            (try
              (ac/update-herate
                db-herate
                {:lahetystila [:s (:ei-lahetetty c/kasittelytilat)]
                 :sahkoposti  [:s sahkoposti]})
              (catch AwsServiceException e
                (log/error e "Virhe tilan päivityksessä herätteelle" db-herate)
                (throw e)))))))

(defn -handleEmailResend
  "Merkistee sähköpostin lähetettäväksi uudestaan, jos osoite löytyy
  SQS-viestistä tai tietokannasta."
  [_ ^SQSEvent event context]
  (log-caller-details-sqs "handleEmailResend" context)
  (let [messages (seq (.getRecords event))]
    (doseq [^SQSEvent$SQSMessage msg messages]
      (try
        (handle-single-herate! (parse-string (.getBody msg) true))
        (catch JsonParseException e
          (log/error e "Virhe viestin lukemisessa:" msg))))))
