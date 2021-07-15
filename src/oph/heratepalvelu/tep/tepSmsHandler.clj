(ns oph.heratepalvelu.tep.tepSmsHandler
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.koski :refer [get-opiskeluoikeus]]
            [oph.heratepalvelu.external.elisa :as elisa]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [cheshire.core :refer [generate-string]]
            [environ.core :refer [env]])
  (:import (com.fasterxml.jackson.core JsonParseException)
           (software.amazon.awssdk.awscore.exception AwsServiceException)
           (clojure.lang ExceptionInfo)))

(gen-class
  :name "oph.heratepalvelu.tep.tepSmsHandler"
  :methods [[^:static handleTepSmsSending
             [com.amazonaws.services.lambda.runtime.events.SQSEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn- update-status-to-db [status ohjaaja_ytunnus_kj_tutkinto niputuspvm]
  (let []
    (try
      (ddb/update-item
        {:ohjaaja_ytunnus_kj_tutkinto [:s ohjaaja_ytunnus_kj_tutkinto]
         :niputuspvm                  [:s niputuspvm]}
        {:update-expr     (str "SET #sms-kasittelytila = :sms-kasittelytila")
         :expr-attr-names {"#sms-kasittelytila" "sms_kasittelytila"}
         :expr-attr-vals {":sms-kasittelytila" status}}
        (:nippu-table env))
      (catch Exception e
        (log/error (str "Error in update-status-to-db for " ohjaaja_ytunnus_kj_tutkinto " " niputuspvm))
        (throw e)))))

(defn -handleTepSmsSending [this event context]
  (log-caller-details-sqs "tepSmsHandler" event context)
  (let [messages (seq (.getRecords event))]
    (doseq [msg messages]
      (try
        (let [msg (parse-string (.getBody msg) true)
              nippu (ddb/get-item {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto msg)]
                                   :niputuspvm                  [:s (:niputuspvm msg)]})
              body (elisa/msg-body (:kyselylinkki nippu) (:oppilaitokset msg) (:muistutus msg))
              phonenumber (:phonenumber msg)
              resp (elisa/send-tep-sms phonenumber body)
              status (get-in resp [:body :messages (keyword phonenumber) :status])]
          ;(update-status-to-db status ohjaaja_ytunnus_kj_tutkinto niputuspvm)
          (log/info resp)
          (log/info resp "SMS sent to " phonenumber))
        (catch JsonParseException e
          (log/error "Virhe viestin lukemisessa: " msg "\n" e))
        (catch AwsServiceException e
          (log/error (str "SMS-viestin lähetysvaiheen kantapäivityksessä tapahtui virhe!"))
          (log/error e))
        (catch ExceptionInfo e
          (if (and
                (> 399 (:status (ex-data e)))
                (< 500 (:status (ex-data e))))
            (do
              (log/error "Client error while sending sms to number " (:phonenumber msg))
              (throw e))
            (do
              (log/error "Server error while sending sms to number " (:phonenumber msg))
              (throw e))))
        (catch Exception e
          (log/error "Unhandle exception " e)
          (throw e))))))
