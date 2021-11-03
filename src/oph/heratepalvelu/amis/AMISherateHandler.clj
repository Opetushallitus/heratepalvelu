(ns oph.heratepalvelu.amis.AMISherateHandler
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.external.koski
             :refer
             [get-opiskeluoikeus-catch-404]]
            [oph.heratepalvelu.common :refer :all]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [environ.core :refer [env]])
  (:import (com.fasterxml.jackson.core JsonParseException)
           (clojure.lang ExceptionInfo)))

(gen-class
  :name "oph.heratepalvelu.amis.AMISherateHandler"
  :methods [[^:static handleAMISherate
             [com.amazonaws.services.lambda.runtime.events.SQSEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn -handleAMISherate [this event context]
  (log-caller-details-sqs "handleAMISherate" context)
  (let [messages (seq (.getRecords event))]
    (doseq [msg messages]
      (try
        (let [herate (parse-string (.getBody msg) true)
              opiskeluoikeus (get-opiskeluoikeus-catch-404
                               (:opiskeluoikeus-oid herate))]
          (if (some? opiskeluoikeus)
            (let [koulutustoimija (get-koulutustoimija-oid opiskeluoikeus)]
              (if (and (check-opiskeluoikeus-suoritus-types? opiskeluoikeus)
                         (check-organisaatio-whitelist?
                           koulutustoimija
                           (date-string-to-timestamp (:alkupvm herate)))
                         (check-sisaltyy-opiskeluoikeuteen? opiskeluoikeus))
                (save-herate herate opiskeluoikeus koulutustoimija)
                (log/info "Ei tallenneta kantaan"
                          (str koulutustoimija "/" (:oppija-oid herate))
                          (str (:kyselytyyppi herate))))
            (log/error "Ei opiskeluoikeutta ID:llä"
                       (:opiskeluoikeus-oid herate)))))
        (catch JsonParseException e
          (log/error "Virhe viestin lukemisessa: " msg "\n" e))
        (catch ExceptionInfo e
          (if (and
                (:status (ex-data e))
                (= 404 (:status (ex-data e))))
            (log/error "404-virhe. Opiskeluoikeus: "
                       (:opiskeluoikeus-oid (parse-string (.getBody msg) true))
                       " error: "
                       e)
            (do (log/error e))))))))
