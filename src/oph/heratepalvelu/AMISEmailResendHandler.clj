(ns oph.heratepalvelu.AMISEmailResendHandler
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.common :refer [kausi]]
            [environ.core :refer [env]]
            [schema.core :as s])
  (:import (com.fasterxml.jackson.core JsonParseException)
           (software.amazon.awssdk.services.dynamodb.model ConditionalCheckFailedException)))

(gen-class
  :name "oph.heratepalvelu.AMISEmailResendHandler"
  :methods [[^:static handleEmailResend
             [com.amazonaws.services.lambda.runtime.events.SQSEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(s/defschema resend-schema
  {:koulutustoimija s/Str
   :oppija-oid s/Str
   :kyselytyyppi s/Str
   :alkupvm s/Str
   :email s/Str})

(defn -handleEmailResend [this event context]
  (let [messages (seq (.getRecords event))]
    (doseq [msg messages]
      (try
        (let [herate (parse-string (.getBody msg) true)
              toimija-oppija (str (:koulutustoimija herate) "/"
                                  (:oppija-oid herate))
              tyyppi-kausi (str (:kyselytyyppi herate) "/"
                                (kausi (:alkupvm herate)))
              schema-check (s/check resend-schema herate)]
          (if (nil? schema-check)
            (try
              (ddb/update-item
                {:toimija_oppija [:s toimija-oppija]
                 :tyyppi_kausi   [:s tyyppi-kausi]}
                {:update-expr     (str "SET #lahetystila = :lahetystila, "
                                       "#sahkoposti = :sahkoposti")
                 :expr-attr-names {"#lahetystila" "lahetystila"
                                   "#sahkoposti" "sahkoposti"}
                 :expr-attr-vals  {":lahetystila" [:s "ei_lahetetty"]
                                   ":sahkoposti" [:s (:email herate)]}
                 :cond-expr (str "attribute_exists(toimija_oppija) AND "
                                 "attribute_exists(tyyppi_kausi)")})
              (catch ConditionalCheckFailedException e
                (log/error "Ei kyselylinkkiä herätteelle"
                           toimija-oppija "/" tyyppi-kausi)))
            (log/error schema-check)))
        (catch JsonParseException e
          (log/error "Virhe viestin lukemisessa: " e))))))