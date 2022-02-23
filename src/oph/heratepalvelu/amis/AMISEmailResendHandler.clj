(ns oph.heratepalvelu.amis.AMISEmailResendHandler
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.amis.AMISCommon :as ac]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.log.caller-log :refer [log-caller-details-sqs]]
            [schema.core :as s])
  (:import (com.fasterxml.jackson.core JsonParseException)
           (software.amazon.awssdk.awscore.exception AwsServiceException)))

;; Ottaa vastaan viestejä ehoksAmisResendQueuesta ja merkitsee kyseessä olevan
;; kyselylinkin sähköpostin lähetettäväksi uudestaan, jos osoite löytyy.

(gen-class
  :name "oph.heratepalvelu.amis.AMISEmailResendHandler"
  :methods [[^:static handleEmailResend
             [com.amazonaws.services.lambda.runtime.events.SQSEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(s/defschema resend-schema
  {:kyselylinkki (s/constrained s/Str not-empty)
   (s/optional-key :sahkoposti) (s/constrained s/Str not-empty)})

(def resend-checker
  (s/checker resend-schema))

(defn update-email-to-resend
  "Päivittää lähetettävän sähköpostin osoitteen ja lähetystilan tietokantaan."
  [toimija-oppija tyyppi-kausi sahkoposti kyselylinkki]
  (try
    (ddb/update-item
      {:toimija_oppija [:s toimija-oppija]
       :tyyppi_kausi   [:s tyyppi-kausi]}
      {:update-expr    "SET #lahetystila = :lahetystila, #sposti = :sposti"
       :expr-attr-names {"#lahetystila" "lahetystila"
                         "#sposti" "sahkoposti"}
       :expr-attr-vals  {":lahetystila" [:s (:ei-lahetetty c/kasittelytilat)]
                         ":sposti" [:s sahkoposti]}})
    (catch AwsServiceException e
      (log/error "Virhe kyselylinkin" kyselylinkki "päivityksessä" e)
      (throw e))))

(defn -handleEmailResend
  "Merkistee sähköpostin lähetettäväksi uudestaan, jos osoite löytyy
  SQS-viestistä tai tietokannasta."
  [this event context]
  (log-caller-details-sqs "handleEmailResend" context)
  (let [messages (seq (.getRecords event))]
    (doseq [msg messages]
      (try
        (let [herate (parse-string (.getBody msg) true)
              kyselylinkki (:kyselylinkki herate)]
          (if (some? (resend-checker herate))
            (log/error {:herate herate :msg (resend-checker herate)})
            (let [item (ac/get-item-by-kyselylinkki kyselylinkki)
                  sahkoposti (or (not-empty (:sahkoposti herate))
                                 (:sahkoposti item))]
              (if item
                (do
                  (when (empty? (:sahkoposti herate))
                    (log/warn "Ei sähköpostia herätteessä" herate
                              ", käytetään dynamoon tallennettua" sahkoposti))
                  (update-email-to-resend (:toimija_oppija item)
                                          (:tyyppi_kausi item)
                                          sahkoposti
                                          kyselylinkki))
                (log/error "Ei kyselylinkkiä" kyselylinkki)))))
        (catch JsonParseException e
          (log/error "Virhe viestin lukemisessa:" msg "\n" e))))))
