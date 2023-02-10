(ns oph.heratepalvelu.amis.AMISherateHandler
  "Käsittelee SQS:stä saatuja AMIS-herätteitä."
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.amis.AMISCommon :as ac]
            [oph.heratepalvelu.common :refer :all]
            [oph.heratepalvelu.external.koski
             :refer
             [get-opiskeluoikeus-catch-404!]]
            [oph.heratepalvelu.log.caller-log :refer :all])
  (:import (com.amazonaws.services.lambda.runtime.events SQSEvent$SQSMessage)
           (com.fasterxml.jackson.core JsonParseException)
           (clojure.lang ExceptionInfo)))

(gen-class
  :name "oph.heratepalvelu.amis.AMISherateHandler"
  :methods [[^:static handleAMISherate
             [com.amazonaws.services.lambda.runtime.events.SQSEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn -handleAMISherate
  "Käsittelee herätteitä ja tallentaa ne tietokantaan, jos ne ovat valideja."
  [_ ^com.amazonaws.services.lambda.runtime.events.SQSEvent event context]
  (log-caller-details-sqs "handleAMISherate" context)
  (let [messages (seq (.getRecords event))]
    (doseq [^SQSEvent$SQSMessage msg messages]
      (try
        (let [herate (parse-string (.getBody msg) true)]
          (if (check-valid-herate-date (:alkupvm herate))
            (let [opiskeluoikeus (get-opiskeluoikeus-catch-404!
                                   (:opiskeluoikeus-oid herate))]
              (if (some? opiskeluoikeus)
                (let [koulutustoimija (get-koulutustoimija-oid opiskeluoikeus)]
                  (if (and (check-opiskeluoikeus-suoritus-types? opiskeluoikeus)
                           (check-organisaatio-whitelist?
                             koulutustoimija
                             (date-string-to-timestamp (:alkupvm herate)))
                           (check-sisaltyy-opiskeluoikeuteen? opiskeluoikeus))
                    (ac/save-herate herate
                                    opiskeluoikeus
                                    koulutustoimija
                                    (:ehoks herate-sources))
                    (log/info "Ei tallenneta kantaan"
                              (str koulutustoimija "/" (:oppija-oid herate))
                              (str (:kyselytyyppi herate)))))
                (log/error "Ei opiskeluoikeutta ID:llä"
                           (:opiskeluoikeus-oid herate))))
            (log/warn "Ei tallenneta kantaan. Alkupvm virheellinen."
                      (str "alkupvm " (:alkupvm herate))
                      (str "opiskeluoikeus-oid " (:opiskeluoikeus-oid herate))
                      (str "oppija-oid " (:oppija-oid herate))
                      (str "kyselytyyppi " (:kyselytyyppi herate)))))
        (catch JsonParseException e
          (log/error "Virhe viestin lukemisessa:" msg "\n" e))
        (catch ExceptionInfo e
          (if (and (:status (ex-data e))
                   (= 404 (:status (ex-data e))))
            (log/error "404-virhe. Opiskeluoikeus:"
                       (:opiskeluoikeus-oid (parse-string (.getBody msg) true))
                       "error:"
                       e)
            (log/error e)))))))
