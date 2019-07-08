(ns oph.heratepalvelu.external.cas-client
  (:refer-clojure :exclude [get])
  (:require [clj-cas.cas :refer :all]
            [environ.core :refer [env]]
            [cheshire.core :as json]
            [clj-http.client :refer [request]]))

(defrecord CasClient [client params session-id])

(def client (atom nil))

(defn- init-client []
  (let [username   (:cas-user env)
        password   (:cas-pwd env)
        cas-url    (:cas-url env)
        cas-params (cas-params "/ryhmasahkoposti-service" username password)
        cas-client (cas-client cas-url)]
    (map->CasClient {:client     cas-client
                     :params     cas-params
                     :session-id (atom nil)})))

(defn- request-with-json-body [request body]
  (-> request
      (assoc-in [:headers "Content-Type"] "application/json")
      (assoc :body (json/generate-string body))))

(defn- create-params [cas-session-id body]
  (cond-> {:headers          {"Cookie" (str "JSESSIONID=" @cas-session-id)
                              "Caller-Id" (:caller-id env)
                              "clientSubSystemCode" (:caller-id env)}
           :redirect-strategy :none}
          (some? body)
          (request-with-json-body body)))

(defn- cas-http [method url options body]
  (when (nil? @client)
    (reset! client (init-client)))
  (let [cas-client     (:client @client)
        cas-params     (:params @client)
        cas-session-id (:session-id @client)]
    (when (nil? @cas-session-id)
      (reset! cas-session-id (.run (.fetchCasSession cas-client cas-params "JSESSIONID"))))
    (let [resp (request (merge {:url url :method method}
                               (create-params cas-session-id body)
                               options))]
      (if (= 302 (:status resp))
        (do
          (reset! cas-session-id (.run (.fetchCasSession cas-client cas-params "JSESSIONID")))
          (request (merge {:url url :method method}
                          (create-params cas-session-id body)
                          options)))
        resp))))

(defn cas-authenticated-get [url & [options]]
  (cas-http :get url options nil))

(defn cas-authenticated-post [url body & [options]]
  (cas-http :post url options body))
