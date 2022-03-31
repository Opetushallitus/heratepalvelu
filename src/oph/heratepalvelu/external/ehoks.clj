(ns oph.heratepalvelu.external.ehoks
  (:require [cheshire.core :refer [generate-string]]
            [environ.core :refer [env]]
            [oph.heratepalvelu.external.cas-client :as cas]
            [oph.heratepalvelu.external.http-client :as client])
  (:import (clojure.lang ExceptionInfo)))

(defn get-hoks-by-opiskeluoikeus
  "Hakee HOKSin opiskeluoikeuden OID:n perusteella."
  [opiskeluoikeus-oid]
  (:data (:body (client/get
                  (str (:ehoks-url env)
                       "hoks/opiskeluoikeus/"
                       opiskeluoikeus-oid)
                  {:headers {:ticket (cas/get-service-ticket
                                       "/ehoks-virkailija-backend"
                                       "cas-security-check")}
                   :as      :json}))))

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
  (:data (:body (client/get
                  (str (:ehoks-url env) "hoks/osaamisen-hankkimistapa/" oht-id)
                  {:headers {:ticket (cas/get-service-ticket
                                       "/ehoks-virkailija-backend"
                                       "cas-security-check")}
                   :as      :json}))))

(defn get-hankintakoulutus-oids
  "Hakee HOKSin hankintakoulutus-OID:t."
  [hoks-id]
  (:body (client/get
           (str (:ehoks-url env) "hoks/" hoks-id "/hankintakoulutukset")
           {:headers {:ticket (cas/get-service-ticket
                                "/ehoks-virkailija-backend"
                                "cas-security-check")}
            :as      :json})))

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

(defn patch-osaamisenhankkimistapa-tep-kasitelty
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
  (client/get
    (str (:ehoks-url env) "heratepalvelu/tyoelamajaksot")
    {:headers {:ticket (cas/get-service-ticket
                         "/ehoks-virkailija-backend"
                         "cas-security-check")}
     :query-params {:start start
                    :end end
                    :limit limit}
     :as :json}))

(defn get-retry-kyselylinkit
  "Pyytää eHOKS-palvelua lähettämään käsittelemättömät AMIS-herätteet
  SQS:iin."
  [start end limit]
  (client/get
    (str (:ehoks-url env) "heratepalvelu/kasittelemattomat-heratteet")
    {:headers {:ticket (cas/get-service-ticket
                         "/ehoks-virkailija-backend"
                         "cas-security-check")}
     :query-params {:start start
                    :end end
                    :limit limit}
     :as :json}))

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
