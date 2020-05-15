(ns oph.heratepalvelu.external.ehoks
  (:require [oph.heratepalvelu.external.http-client :as client]
            [oph.heratepalvelu.external.cas-client :as cas]
            [environ.core :refer [env]]
            [clojure.tools.logging :as log]
            [cheshire.core :refer [generate-string]]))

(defn get-hoks-by-opiskeluoikeus [opiskeluoikeus-oid]
  (:data
    (:body (client/get
             (str (:ehoks-url env) "hoks/opiskeluoikeus/" opiskeluoikeus-oid)
             {:headers {:ticket (cas/get-service-ticket
                                  "/ehoks-virkailija-backend"
                                  "cas-security-check")}
              :as :json}))))

(defn add-kyselytunnus-to-hoks [hoks-id data]
  (client/post
    (str (:ehoks-url env) "hoks/" hoks-id "/kyselylinkki")
    {:headers {:ticket (cas/get-service-ticket
                         "/ehoks-virkailija-backend"
                         "cas-security-check")}
     :content-type "application/json"
     :body (generate-string data)
     :as :json}))

(defn get-hankintakoulutus-oids [hoks-id]
  (:data
    (:body
      (client/get
        (str (:ehoks-url env) "hoks/" hoks-id "/hankintakoulutukset")
        {:headers {:ticket (cas/get-service-ticket
                             "/ehoks-virkailija-backend"
                             "cas-security-check")}
         :as :json}))))