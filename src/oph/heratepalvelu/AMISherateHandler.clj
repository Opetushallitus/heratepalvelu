(ns oph.heratepalvelu.AMISherateHandler
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.external.koski :refer [get-opiskeluoikeus]]
            [oph.heratepalvelu.common :refer :all]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [environ.core :refer [env]])
  (:import (com.fasterxml.jackson.core JsonParseException)
           (clojure.lang ExceptionInfo)))

(gen-class
  :name "oph.heratepalvelu.AMISherateHandler"
  :methods [[^:static handleAMISherate
             [com.amazonaws.services.lambda.runtime.events.SQSEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn -handleAMISherate [this event context]
  (log-caller-details "handleAMISherate" event context)
  (let [messages (seq (.getRecords event))]
    (doseq [msg messages]
      (try
        (let [herate (parse-string (.getBody msg) true)
              opiskeluoikeus (get-opiskeluoikeus (:opiskeluoikeus-oid herate))
              koulutustoimija (get-koulutustoimija-oid opiskeluoikeus)]
          (when (and (check-suoritus-type?
                       (first (seq (:suoritukset opiskeluoikeus))))
                     (check-organisaatio-whitelist?
                       koulutustoimija (date-string-to-timestamp
                                         (:alkupvm herate)))
                     (nil? (:sisältyyOpiskeluoikeuteen opiskeluoikeus)))
            (save-herate herate opiskeluoikeus)))
        (catch JsonParseException e
          (log/error "Virhe viestin lukemisessa: " e))
        (catch ExceptionInfo e
          (if (and
                (:status (ex-data e))
                (< 399 (:status (ex-data e)))
                (> 500 (:status (ex-data e))))
            (if (= 404 (:status (ex-data e)))
              (log/error "Ei opiskeluoikeutta " (:opiskeluoikeus-oid (parse-string (.getBody msg) true)))
              (log/error "Unhandled client error: " e))
            (do (log/error e)
                (throw e))))))))