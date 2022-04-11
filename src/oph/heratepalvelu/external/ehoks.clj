(ns oph.heratepalvelu.external.ehoks
  "Wrapperit eHOKSin REST-rajapinnan ympäri."
  (:require [cheshire.core :refer [generate-string]]
            [environ.core :refer [env]]
            [oph.heratepalvelu.external.cas-client :as cas]
            [oph.heratepalvelu.external.http-client :as client])
  (:import (clojure.lang ExceptionInfo)))

(defn- ehoks-query-base-options
  "Alustavat optiot ehoks-kyselyyn."
  []
  {:headers {:ticket (cas/get-service-ticket "/ehoks-virkailija-backend"
                                                    "cas-security-check")}
   :as      :json})

(defn- ehoks-get
  "Tekee GET-kysely ehoksiin."
  [uri-path options]
  (client/get (str (:ehoks-url env) uri-path)
              (merge (ehoks-query-base-options) options)))

(defn get-hoks-by-opiskeluoikeus
  "Hakee HOKSin opiskeluoikeuden OID:n perusteella."
  [oo-oid]
  (:data (:body (ehoks-get (str "hoks/opiskeluoikeus/" oo-oid) {}))))

(defn add-kyselytunnus-to-hoks
  "Lisää kyselytunnuksen HOKSiin. Tekee yhden retryn automaattiesti."
  [hoks-id data]
  (let [action (fn [] (client/post
                        (str (:ehoks-url env) "hoks/" hoks-id "/kyselylinkki")
                        {:headers {:ticket (cas/get-service-ticket
                                             "/ehoks-virkailija-backend"
                                             "cas-security-check")}
                         :content-type "application/json"
                         :body (generate-string data)
                         :as :json}))]
    (try (action)
         (catch ExceptionInfo e (action)))))

(defn get-osaamisen-hankkimistapa-by-id
  "Hakee osaamisen hankkimistavan ID:n perusteella."
  [oht-id]
  (:data (:body (ehoks-get (str "hoks/osaamisen-hankkimistapa/" oht-id) {}))))

(defn get-hankintakoulutus-oids
  "Hakee HOKSin hankintakoulutus-OID:t."
  [hoks-id]
  (:body (ehoks-get (str "hoks/" hoks-id "/hankintakoulutukset") {})))

(defn add-lahetys-info-to-kyselytunnus
  "Lisää lähetysinfon kyselytunnukseen. Tekee yhden retryn jos vastaus on virhe
  ja status ei ole 404."
  [data]
  (let [action (fn [] (client/patch
                        (str (:ehoks-url env) "hoks/kyselylinkki")
                        {:headers {:ticket (cas/get-service-ticket
                                             "/ehoks-virkailija-backend"
                                             "cas-security-check")}
                         :content-type "application/json"
                         :body (generate-string data)
                         :as :json}))]
    (try (action)
         (catch ExceptionInfo e
           (if (not= 404 (:status (ex-data e)))
             (action)
             (throw e))))))

(defn patch-oht-tep-kasitelty
  "Merkitsee osaamisen hankkimistavan käsitellyksi eHOKS-palvelussa."
  [id]
  (client/patch
    (str (:ehoks-url env)
         "heratepalvelu/osaamisenhankkimistavat/"
         id
         "/kasitelty")
    {:headers {:ticket (cas/get-service-ticket
                         "/ehoks-virkailija-backend"
                         "cas-security-check")}
     :content-type "application/json"
     :as :json}))

(defn get-paattyneet-tyoelamajaksot
  "Pyytää eHOKS-palvelua lähettämään käsittelemättömät TEP-jaksot SQS:iin."
  [start end limit]
  (ehoks-get "heratepalvelu/tyoelamajaksot" {:query-params {:start start
                                                            :end end
                                                            :limit limit}}))

(defn get-retry-kyselylinkit
  "Pyytää eHOKS-palvelua lähettämään käsittelemättömät AMIS-herätteet
  SQS:iin."
  [start end limit]
  (ehoks-get "heratepalvelu/kasittelemattomat-heratteet"
             {:query-params {:start start
                             :end end
                             :limit limit}}))

(defn patch-amisherate-kasitelty
  "Merkitsee HOKSin aloitus- tai päättöherätteen käsitellyksi."
  [url-tyyppi-element id]
  (client/patch
    (str (:ehoks-url env) "heratepalvelu/hoksit/" id "/" url-tyyppi-element)
    {:headers {:ticket (cas/get-service-ticket
                         "/ehoks-virkailija-backend"
                         "cas-security-check")}
     :content-type "application/json"
     :as :json}))

(defn patch-amis-aloitusherate-kasitelty
  "Merkitsee HOKSin aloitusherätteen käsitellyksi."
  [id]
  (patch-amisherate-kasitelty "aloitusherate-kasitelty" id))

(defn patch-amis-paattoherate-kasitelty
  "Merkitsee HOKSin päättöherätteen käsitellyksi."
  [id]
  (patch-amisherate-kasitelty "paattoherate-kasitelty" id))

(defn resend-aloitusheratteet
  "Pyytää eHOKS-palvelua lähettämään aloitusherätteet uudelleen tietylle
  aikavälille."
  [start end]
  (client/post
    (str (:ehoks-url env) "heratepalvelu/hoksit/resend-aloitusherate")
    {:headers {:ticket (cas/get-service-ticket "/ehoks-virkailija-backend"
                                               "cas-security-check")}
     :query-params {:from start :to end}
     :as :json}))

(defn resend-paattoheratteet
  "Pyytää eHOKS-palvelua lähettämään päättöherätteet uudelleen tietylle
  aikavälille."
  [start end]
  (client/post
    (str (:ehoks-url env) "heratepalvelu/hoksit/resend-paattoherate")
    {:headers {:ticket (cas/get-service-ticket "/ehoks-virkailija-backend"
                                               "cas-security-check")}
     :query-params {:from start :to end}
     :as :json}))

(defn update-ehoks-opiskeluoikeudet
  "Päivittää aktiivisten hoksien opiskeluoikeudet Koskesta"
  []
  (client/post
    (str (:ehoks-url env) "heratepalvelu/opiskeluoikeus-update")
    {:headers {:ticket (cas/get-service-ticket "/ehoks-virkailija-backend"
                                               "cas-security-check")}
     :as :json}))
