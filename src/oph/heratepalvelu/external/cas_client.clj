(ns oph.heratepalvelu.external.cas-client
  "Wrapperit CAS-clientin ympäri."
  (:refer-clojure :exclude [get])
  (:require [clj-cas.cas :refer :all]
            [clj-cas.client :as cl]
            [environ.core :refer [env]]
            [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.external.http-client :refer [request]]
            [oph.heratepalvelu.external.aws-ssm :as ssm])
  (:import (fi.vm.sade.utils.cas CasClient
                                 CasParams
                                 TicketGrantingTicketClient
                                 ServiceTicketClient)
           (org.http4s Uri)))

(defrecord CasClientWrapper [client params session-id])

(def client
  "CAS-client -objekti (atom, joka saa sisältää nil)."
  (atom nil))

(def ^:private pwd
  "CAS-clientin autentikoinnin salasana."
  (delay
    (ssm/get-secret (str "/" (:stage env) "/services/heratepalvelu/cas-pwd"))))

(defn init-client
  "Luo ja asentaa uuden CasClientWrapper-rekordin."
  []
  (let [username   (:cas-user env)
        password   @pwd
        cas-url    (:cas-url env)
        cas-params (cas-params "/ryhmasahkoposti-service" username password)
        cas-client (cas-client cas-url (:caller-id env))]
    (map->CasClientWrapper {:client     cas-client
                            :params     cas-params
                            :session-id (atom nil)})))

(defn request-with-json-body
  "Muuttaa request bodyn JSON-muodoksi ja lisää sen requestiin."
  [request body]
  (-> request
      (assoc-in [:headers "Content-Type"] "application/json")
      (assoc :body (json/generate-string body))))

(defn create-params
  "Luo objekti, joka sisältää parametreja requestille."
  [cas-session-id body]
  (cond-> {:headers          {"Caller-Id" (:caller-id env)
                              "clientSubSystemCode" (:caller-id env)
                              "CSRF" (:caller-id env)}
           :cookies          {"CSRF" {:value (:caller-id env)
                                      :path "/"}
                              "JSESSIONID" {:value (str @cas-session-id)
                                            :path "/"}}
           :redirect-strategy :none}
    (some? body)
    (request-with-json-body body)))

(defn cas-http
  "Tekee Cas-autentikoidun requestin."
  [method url options body]
  (when (nil? @client)
    (reset! client (init-client)))
  (let [cas-client     ^CasClient (:client @client)
        cas-params     (:params @client)
        cas-session-id (:session-id @client)]
    (when (nil? @cas-session-id)
      (reset! cas-session-id
              (.run (.fetchCasSession cas-client cas-params "JSESSIONID"))))
    (let [resp (request (merge {:url url :method method}
                               (create-params cas-session-id body)
                               options))]
      (if (= 302 (:status resp))
        (do
          (reset! cas-session-id
                  (.run (.fetchCasSession cas-client cas-params "JSESSIONID")))
          (request (merge {:url url :method method}
                          (create-params cas-session-id body)
                          options)))
        resp))))

(defn cas-authenticated-get
  "Tekee Cas-autentikoidun GET-requestin."
  [url & [options]]
  (cas-http :get url options nil))

(defn cas-authenticated-post
  "Tekee Cas-autentikoidun POST-requestin."
  [url body & [options]]
  (cas-http :post url options body))

(defn- get-uri
  "Muuttaa stringin Uri-objektiksi."
  [uri]
  (-> (Uri/fromString uri)
      (.toOption)
      (.get)))

(def tgt
  "Ticket granting ticket client -objekti (atom, joka saa sisältää nil)."
  (atom nil))

(defn get-tgt
  [cas-uri params]
  (.run (TicketGrantingTicketClient/getTicketGrantingTicket
          cas-uri cl/client params (:caller-id env))))

(defn- refresh-tgt
  [cas-uri params]
  (reset! tgt (get-tgt cas-uri params)))

(defn get-st
  [service-uri]
  (.run (ServiceTicketClient/getServiceTicketFromTgt
          cl/client service-uri (:caller-id env) @tgt)))

(defn- try-to-get-st
  [service-uri cas-uri params]
  (try
    (get-st service-uri)
    (catch Exception e
      (if (= (:status (ex-data e)) 404)
        (do
          (log/info (str "TGT not found, refreshing TGT"))
          (refresh-tgt cas-uri params)
          (get-st service-uri))
        (throw e)))))

(defn get-service-ticket
  "Hakee service ticketin palvelusta."
  [service suffix]
  (let [username    (:cas-user env)
        password    @pwd
        params      (CasParams/apply service suffix username password)
        service-uri (get-uri (str (:virkailija-url env) service "/" suffix))
        cas-uri     (get-uri (:cas-url env))]
    (when (nil? @tgt)
      (refresh-tgt cas-uri params))
    (try-to-get-st service-uri cas-uri params)))
