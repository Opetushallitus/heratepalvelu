(ns oph.heratepalvelu.tep.tepSmsHandler
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.http-client :as client]
            [oph.heratepalvelu.external.koski :refer [get-opiskeluoikeus]]
            [oph.heratepalvelu.common :refer :all]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.external.aws-ssm :as ssm]
            [oph.heratepalvelu.common :as c]
            [cheshire.core :refer [generate-string]]
            [environ.core :refer [env]])
  (:import (com.fasterxml.jackson.core JsonParseException)
           (com.google.i18n.phonenumbers PhoneNumberUtil)
           (com.google.i18n.phonenumbers NumberParseException)
           (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.tep.tepSmsHandler"
  :methods [[^:static handleTepSmsSending
             [com.amazonaws.services.lambda.runtime.events.SQSEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(def ^:private apikey (delay
                     (ssm/get-secret
                       (str "/" (:stage env)
                            "/services/heratepalvelu/elisa-sms-dialogi-key"))))

(defn- update-status-to-db [status ohjaaja_ytunnus_kj_tutkinto niputuspvm]
  (let [sms-status (if (= status 200)
                     (:success c/kasittelytilat)
                     (:failed c/kasittelytilat))]
    (try
      (ddb/update-item
        {:ohjaaja_ytunnus_kj_tutkinto [:s ohjaaja_ytunnus_kj_tutkinto]
         :niputuspvm                  [:s niputuspvm]}
        {:update-expr     (str "SET #sms-kasittelytila = :sms-kasittelytila")
         :expr-attr-names {"#sms-kasittelytila" "sms-kasittelytila"}
         :expr-attr-vals {":sms-kasittelytila" sms-status}}
        (:nippu-table env))
      (catch Exception e
        (log/error (str "Error in update-status-to-db for " ohjaaja_ytunnus_kj_tutkinto " " niputuspvm))
        (throw e)))))

(defn- valid-number? [number]
  (let [utilobj (PhoneNumberUtil/getInstance)
        numberobj (.parse utilobj number "FI")]
    (and (empty? (filter (fn [x] (Character/isLetter x)) number))
         (.isValidNumber utilobj numberobj)
         (= (str (.getNumberType utilobj numberobj))
            "MOBILE"))))

(defn send-tep-sms [number message]
  (try
    (when (valid-number? number)
      (let [body {:sender "Opetushallitus"
                  :destination [number]
                  :text message}
            resp (client/post
                   (str "https://viestipalvelu-api.elisa.fi/api/v1/")
                   {:headers {
                              :Authorization  (str "apikey " @apikey)
                              :content-type "application/json"
                              }
                    :body         (generate-string body)})]
        resp))
    (catch NumberParseException e
      (log/error "PhoneNumberUtils failed to parse phonenumber " number)
      (throw e))
    (catch Exception e
      (throw e))))

(defn -handleTepSmsSending [this event context]
  (log-caller-details-sqs "tepSmsHandler" event context)
  (let [messages (seq (.getRecords event))]
    (doseq [msg messages]
      (try
        (let [msg (parse-string (.getBody msg) true)
              body (:body msg)
              phonenumber (:phonenumber msg)
              ohjaaja_ytunnus_kj_tutkinto (:ohjaaja_ytunnus_kj_tutkinto msg)
              niputuspvm (:niputuspvm msg)
              resp (send-tep-sms phonenumber body)
              status (:status resp)]
          (do
            ;; (update-status-to-db status ohjaaja_ytunnus_kj_tutkinto niputuspvm)
            (if (= status 200)
              (do
                (log/info resp)
                (log/info resp "SMS sent to " phonenumber))
              (if (and
                    (> 399 status)
                    (< 500 status))
                (do
                  (log/error "Client error while sending sms to number " phonenumber)
                  (throw (ex-info (str "Client error while sending sms to number " phonenumber) resp)))
                (do
                  (log/error "Server error while sending sms to number " phonenumber)
                  (throw (ex-info (str "Server error while sending sms to number " phonenumber) resp)))))))
        (catch JsonParseException e
          (log/error "Virhe viestin lukemisessa: " msg "\n" e))
        (catch AwsServiceException e
          (log/error (str "SMS-viestin lähetysvaiheen kantapäivityksessä tapahtui virhe!"))
          (log/error e))
        (catch Exception e
          (log/error "Unhandle exception " e)
          (throw e))))))
