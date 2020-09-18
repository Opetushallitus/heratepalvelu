(ns oph.heratepalvelu.AMISEmailResendHandler
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.common :refer [kausi lahetystilat]]
            [environ.core :refer [env]]
            [schema.core :as s])
  (:import (com.fasterxml.jackson.core JsonParseException)
           (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.AMISEmailResendHandler"
  :methods [[^:static handleEmailResend
             [com.amazonaws.services.lambda.runtime.events.SQSEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(s/defschema resend-schema
  {:kyselylinkki (s/constrained s/Str not-empty)
   (s/optional-key :sahkoposti) (s/constrained s/Str not-empty)})

(def resend-checker
  (s/checker resend-schema))

(defn -handleEmailResend [this event context]
  (log-caller-details "handleEmailResend" event context)
  (let [messages (seq (.getRecords event))]
    (doseq [msg messages]
      (try
        (let [herate (parse-string (.getBody msg) true)
              kyselylinkki (:kyselylinkki herate)]
          (if (some? (resend-checker herate))
            (log/error {:herate herate :msg (resend-checker herate)})
            (try
              (let [item (first (ddb/query-items
                                  {:kyselylinkki [:eq [:s kyselylinkki]]}
                                  {:index "resendIndex"}))
                    toimija-oppija (:toimija_oppija item)
                    tyyppi-kausi (:tyyppi_kausi item)
                    sahkoposti (or (not-empty (:sahkoposti herate))
                                   (:sahkoposti item))]
                (if item
                  (do
                    (when (empty? (:sahkoposti herate))
                      (log/warn "Ei sähköpostia herätteessä " herate
                                ", käytetään dynamoon tallennettua " sahkoposti))
                    (ddb/update-item
                      {:toimija_oppija [:s toimija-oppija]
                       :tyyppi_kausi   [:s tyyppi-kausi]}
                      {:update-expr     (str "SET #lahetystila = :lahetystila, "
                                             "#sahkoposti = :sahkoposti")
                       :expr-attr-names {"#lahetystila" "lahetystila"
                                         "#sahkoposti" "sahkoposti"}
                       :expr-attr-vals  {":lahetystila" [:s (:ei-lahetetty lahetystilat)]
                                         ":sahkoposti" [:s sahkoposti]}}))
                  (log/error "Ei kyselylinkkiä " kyselylinkki)))
              (catch AwsServiceException e
                (log/error "Virhe kyselylinkin (" kyselylinkki
                           ") päivityksessä tai hakemisessa" e)
                (throw e)))))
        (catch JsonParseException e
          (log/error "Virhe viestin lukemisessa: " msg "\n" e))))))