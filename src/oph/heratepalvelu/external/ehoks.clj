(ns oph.heratepalvelu.external.ehoks
  (:require [oph.heratepalvelu.external.http-client :as client]
            [oph.heratepalvelu.external.cas-client :as cas]
            [environ.core :refer [env]]
            [cheshire.core :refer [generate-string]]))

(defn get-hoks-by-opiskeluoikeus [opiskeluoikeus-oid]
  (:data
    (:body
      (client/get
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
  (:body
    (client/get
      (str (:ehoks-url env) "hoks/" hoks-id "/hankintakoulutukset")
      {:headers {:ticket (cas/get-service-ticket
                           "/ehoks-virkailija-backend"
                           "cas-security-check")}
       :as :json})))

(defn add-lahetys-info-to-kyselytunnus [data]
  (client/patch
    (str (:ehoks-url env) "hoks/kyselylinkki")
    {:headers {:ticket (cas/get-service-ticket
                         "/ehoks-virkailija-backend"
                         "cas-security-check")}
     :content-type "application/json"
     :body (generate-string data)
     :as :json}))

(defn patch-osaamisenhankkimistapa-tep-kasitelty [id]
  (client/patch
    (str (:ehoks-url env) (str "osaamisenhankkimistavat/" id "/kasitelty"))
    {:headers {:ticket (cas/get-service-ticket
                         "/ehoks-virkailija-backend"
                         "cas-security-check")}
     :content-type "application/json"
     :as :json}))

(defn get-paattyneet-tyoelamajaksot [start end limit]
  (client/get
    (str (:ehoks-url env) (str "tyoelamajaksot"))
    {:headers {:ticket (cas/get-service-ticket
                         "/ehoks-virkailija-backend"
                         "cas-security-check")}
     :query-params {:start start
                    :end end
                    :limit limit}
     :as :json}))
