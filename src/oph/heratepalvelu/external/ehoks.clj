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
  {:headers {:ticket (cas/get-service-ticket
                       (or (:ehoks-cas-base env) "/ehoks-virkailija-backend")
                       "cas-security-check")}
   :as      :json})

(defn- retry-on-401
  [operation]
  (try
    (operation)
    (catch Exception e
      (if (= (:status (ex-data e)) 401)
        (operation)
        (throw e)))))

(defn- ehoks-get
  "Tekee GET-kyselyn ehoksiin."
  ([uri-path] (ehoks-get uri-path {}))
  ([uri-path options]
   (retry-on-401
     #(client/get (str (:ehoks-url env) uri-path)
                  (merge (ehoks-query-base-options) options)))))

(defn- ehoks-post
  "Tekee POST-kyselyn ehoksiin"
  ([uri-path] (ehoks-post uri-path {}))
  ([uri-path options]
   (retry-on-401
     #(client/post (str (:ehoks-url env) uri-path)
                   (merge (ehoks-query-base-options) options)))))

(defn- ehoks-patch
  "Tekee PATCH-kyselyn ehoksiin"
  ([uri-path] (ehoks-patch uri-path {}))
  ([uri-path options]
   (retry-on-401
     #(client/patch (str (:ehoks-url env) uri-path)
                    (merge (assoc (ehoks-query-base-options)
                                  :content-type "application/json")
                           options)))))

(defn- ehoks-delete
  "Tekee DELETE-kyselyn ehoksiin."
  ([uri-path] (ehoks-delete uri-path {}))
  ([uri-path options]
   (retry-on-401
     #(client/delete (str (:ehoks-url env) uri-path)
                     (merge (ehoks-query-base-options) options)))))

(defn get-hoks-by-opiskeluoikeus
  "Hakee HOKSin opiskeluoikeuden OID:n perusteella."
  [oo-oid]
  (:data (:body (ehoks-get (str "hoks/opiskeluoikeus/" oo-oid)))))

(defn add-kyselytunnus-to-hoks
  "Lisää kyselytunnuksen HOKSiin. Tekee yhden retryn automaattiesti."
  [hoks-id data]
  (let [action (fn [] (ehoks-post (str "hoks/" hoks-id "/kyselylinkki")
                                  {:content-type "application/json"
                                   :body         (generate-string data)}))]
    (try (action)
         (catch ExceptionInfo e (action)))))

(defn get-osaamisen-hankkimistapa-by-id
  "Hakee osaamisen hankkimistavan ID:n perusteella."
  [oht-id]
  (:data (:body (ehoks-get (str "hoks/osaamisen-hankkimistapa/" oht-id)))))

(defn get-hankintakoulutus-oids
  "Hakee HOKSin hankintakoulutus-OID:t."
  [hoks-id]
  (:body (ehoks-get (str "hoks/" hoks-id "/hankintakoulutukset"))))

(defn add-lahetys-info-to-kyselytunnus
  "Lisää lähetysinfon kyselytunnukseen. Tekee yhden retryn jos vastaus on virhe
  ja status ei ole 404."
  [data]
  (let [action (fn [] (ehoks-patch "hoks/kyselylinkki"
                                   {:body (generate-string data)}))]
    (try (action)
         (catch ExceptionInfo e
           (if (not= 404 (:status (ex-data e)))
             (action)
             (throw e))))))

(defn patch-oht-tep-kasitelty
  "Merkitsee osaamisen hankkimistavan käsitellyksi eHOKS-palvelussa."
  [id]
  (ehoks-patch (str "heratepalvelu/osaamisenhankkimistavat/" id "/kasitelty")))

(defn get-paattyneet-tyoelamajaksot
  "Pyytää eHOKS-palvelua lähettämään käsittelemättömät TEP-jaksot SQS:iin."
  [start end limit]
  (ehoks-get "heratepalvelu/tyoelamajaksot" {:query-params {:start start
                                                            :end end
                                                            :limit limit}}))

(defn send-kasittelemattomat-heratteet!
  "Pyytää eHOKS-palvelua lähettämään käsittelemättömät AMIS-herätteet
  SQS:iin."
  [start end limit]
  (ehoks-get "heratepalvelu/kasittelemattomat-heratteet"
             {:query-params {:start start
                             :end end
                             :limit limit}}))

(defn get-tyoelamajaksot-active-between!
  "Pyytää eHOKS-palvelusta työelämäjaksot, jotka ovat tai olivat voimassa tietyn
  aikavälin sisällä ja jotka kuuluvat tietylle oppijalle."
  [oppija start end]
  (:data (:body (ehoks-get "heratepalvelu/tyoelamajaksot-active-between"
                           {:query-params {:oppija oppija
                                           :start start
                                           :end end}}))))

(defn patch-amis-aloitusherate-kasitelty
  "Merkitsee HOKSin aloitusherätteen käsitellyksi."
  [id]
  (ehoks-patch (str "heratepalvelu/hoksit/" id "/aloitusherate-kasitelty")))

(defn patch-amis-paattoherate-kasitelty
  "Merkitsee HOKSin päättöherätteen käsitellyksi."
  [id]
  (ehoks-patch (str "heratepalvelu/hoksit/" id "/paattoherate-kasitelty")))

(defn resend-aloitusheratteet
  "Pyytää eHOKS-palvelua lähettämään aloitusherätteet uudelleen tietylle
  aikavälille."
  [start end]
  (ehoks-post "heratepalvelu/hoksit/resend-aloitusherate"
              {:query-params {:from start :to end}}))

(defn resend-paattoheratteet
  "Pyytää eHOKS-palvelua lähettämään päättöherätteet uudelleen tietylle
  aikavälille."
  [start end]
  (ehoks-post "heratepalvelu/hoksit/resend-paattoherate"
              {:query-params {:from start :to end}}))

(defn update-ehoks-opiskeluoikeudet
  "Päivittää aktiivisten hoksien opiskeluoikeudet Koskesta"
  []
  (ehoks-post "heratepalvelu/opiskeluoikeus-update" {}))

(defn post-henkilomodify-event
  "Lähettää tiedon henkilön tietojen muutoksesta eHOKS-palveluun."
  [oppija-oid]
  (ehoks-post (str "heratepalvelu/onrmodify")
              {:query-params {:oid oppija-oid}}))

(defn delete-tyopaikkaohjaajan-yhteystiedot
  "Poistaa työpaikkaohjaajan yhteystiedot yli kolme kuukautta sitten
  päättyneistä työelämäjaksoista"
  []
  (ehoks-delete "heratepalvelu/tyopaikkaohjaajan-yhteystiedot" {}))

(defn delete-opiskelijan-yhteystiedot
  "Poistaa opiskelijan yhteystiedot yli kolme kuukautta sitten
  päättyneistä hokseista"
  []
  (ehoks-delete "heratepalvelu/opiskelijan-yhteystiedot" {}))
