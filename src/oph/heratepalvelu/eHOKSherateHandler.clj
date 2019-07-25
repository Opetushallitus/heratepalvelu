(ns oph.heratepalvelu.eHOKSherateHandler
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.external.koski :refer [get-opiskeluoikeus]]
            [oph.heratepalvelu.common :refer :all]
            [environ.core :refer [env]]
            [schema.core :as s])
  (:import (com.fasterxml.jackson.core JsonParseException)
           (clojure.lang ExceptionInfo)))

(gen-class
  :name "oph.heratepalvelu.eHOKSherateHandler"
  :methods [[^:static handleHOKSherate
             [com.amazonaws.services.lambda.runtime.events.SQSEvent] void]])

(s/defschema herate-schema
  {:ehoks-id s/Num
   :kyselytyyppi s/Str
   :opiskeluoikeus-oid s/Str
   :oppija-oid s/Str
   :sahkoposti s/Str
   :alkupvm s/Str})

(defn -handleHOKSherate [this event]
  (let [messages (seq (.getRecords event))]
    (doseq [msg messages]
      (try
        (let [hoks (parse-string (.getBody msg) true)
              opiskeluoikeus (get-opiskeluoikeus (:opiskeluoikeus-oid hoks))
              koulutustoimija (get-koulutustoimija-oid opiskeluoikeus)]
          (if
            (and (nil? (s/check herate-schema hoks))
                 (check-suoritus-type? (first (seq (:suoritukset opiskeluoikeus))))
                 (check-organisaatio-whitelist? koulutustoimija))
            (save-herate hoks opiskeluoikeus
                         (:kyselytyyppi hoks)
                         (:alkupvm hoks))
            (log/error (s/check herate-schema hoks))))
        (catch JsonParseException e
          (log/error "Virheellinen viesti " e))
        (catch ExceptionInfo e
          (if (and
                (:status (:data e))
                (< 399 (:status (:data e)))
                (> 500 (:status (:data e))))
            (log/error "Unhandled client error " e)
            (throw e)))))))
