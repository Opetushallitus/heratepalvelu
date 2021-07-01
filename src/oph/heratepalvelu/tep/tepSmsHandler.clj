(ns oph.heratepalvelu.tep.tepSmsHandler
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.external.http-client :as client]
            [oph.heratepalvelu.external.koski :refer [get-opiskeluoikeus]]
            [oph.heratepalvelu.common :refer :all]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.external.aws-ssm :as ssm]
            [cheshire.core :refer [generate-string]]
            [environ.core :refer [env]])
  (:import (com.fasterxml.jackson.core JsonParseException)
           (com.google.i18n.phonenumbers PhoneNumberUtil)
           (com.google.i18n.phonenumbers NumberParseException)))

(gen-class
  :name "oph.heratepalvelu.tep.tepSmsHandler"
  :methods [[^:static tepSmsHandler
             [com.amazonaws.services.lambda.runtime.events.SQSEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(def ^:private apikey (delay
                     (ssm/get-secret
                       (str "/" (:stage env)
                            "/services/heratepalvelu/elisa-sms-dialogi-key"))))

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

(defn -tepSmsHandler [this event context]
  (log-caller-details-sqs "tepSmsHandler" event context)
  (let [messages (seq (.getRecords event))]
    (doseq [msg messages]
      (try
        (let [msg (parse-string (.getBody msg) true)
              body (:body msg)
              phonenumber (:phonenumber msg)
              resp (send-tep-sms phonenumber body)
              status (:status resp)]
          (if (= status 200)
            (println "SMS sent to " phonenumber)
            (if (and
                  (> 399 status)
                  (< 500 status))
              (do
                (log/error "Client error while sending sms to number " phonenumber)
                (throw (ex-info (str "Client error while sending sms to number " phonenumber) resp)))
              (do
                (log/error "Server error while sending sms to number " phonenumber)
                (throw (ex-info (str "Server error while sending sms to number " phonenumber) resp))))))
        (catch JsonParseException e
          (log/error "Virhe viestin lukemisessa: " msg "\n" e))
        (catch Exception e
          (log/error "Unhandle exception " e)
          (throw e))))))
