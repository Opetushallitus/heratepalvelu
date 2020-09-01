(ns oph.heratepalvelu.external.arvo
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.external.http-client :as client]
            [oph.heratepalvelu.external.koski :as koski]
            [oph.heratepalvelu.external.organisaatio :as org]
            [oph.heratepalvelu.external.ehoks :as ehoks]
            [cheshire.core :refer [generate-string]]
            [clojure.string :as str]
            [oph.heratepalvelu.external.aws-ssm :as ssm])
  (:import (clojure.lang ExceptionInfo)))

(def ^:private pwd (delay
                     (ssm/get-secret
                       (str "/" (:stage env)
                            "/services/heratepalvelu/arvo-pwd"))))

(defn get-toimipiste [suoritus]
  (let [oid (:oid (:toimipiste suoritus))
        org (org/get-organisaatio oid)
        org-tyypit (:tyypit org)]
    (if (some #{"organisaatiotyyppi_03"} org-tyypit)
      oid
      nil)))

(defn get-hankintakoulutuksen-toteuttaja [ehoks-id]
  (let [oids (ehoks/get-hankintakoulutus-oids ehoks-id)]
    (when (not-empty oids)
      (if (> (count oids) 1)
        (log/warn "Enemmän kuin yksi linkitetty opiskeluoikeus! HOKS-id: " ehoks-id)
        (let [opiskeluoikeus (koski/get-opiskeluoikeus (first oids))
              toteuttaja-oid
              (get-in
                opiskeluoikeus
                [:koulutustoimija :oid])]
          (log/info "Hoks " ehoks-id ", hankintakoulutuksen toteuttaja:" toteuttaja-oid)
          toteuttaja-oid)))))

(defn build-arvo-request-body [herate
                               opiskeluoikeus
                               request-id
                               koulutustoimija
                               suoritus]
  {:vastaamisajan_alkupvm   (:alkupvm herate)
   :kyselyn_tyyppi          (:kyselytyyppi herate)
   :tutkintotunnus          (get-in suoritus [:koulutusmoduuli
                                              :tunniste
                                              :koodiarvo])
   :tutkinnon_suorituskieli (str/lower-case
                              (:koodiarvo (:suorituskieli suoritus)))
   :koulutustoimija_oid     koulutustoimija
   :oppilaitos_oid          (:oid (:oppilaitos opiskeluoikeus))
   :request_id              request-id
   :toimipiste_oid          (get-toimipiste suoritus)
   :hankintakoulutuksen_toteuttaja (get-hankintakoulutuksen-toteuttaja
                                     (:ehoks-id herate))})

(defn get-kyselylinkki [data]
  (try
    (let [resp (client/post
                 (:arvo-url env)
                 {:content-type "application/json"
                  :body (generate-string data)
                  :basic-auth [(:arvo-user env) @pwd]
                  :as :json})]
      (:body resp))
    (catch ExceptionInfo e
      (log/error e)
      (when-not (= 404 (:status (ex-data e)))
        (throw e)))))

(defn deactivate-kyselylinkki [linkki]
  (let [tunnus (last (str/split linkki #"/"))]
    (client/delete (str (:arvo-url env) "/" tunnus)
                   {:basic-auth [(:arvo-user env) @pwd]})))

(defn get-kyselylinkki-status [linkki]
  (let [tunnus (last (str/split linkki #"/"))]
    (:body (client/get (str (:arvo-url env) "/status/" tunnus)
                       {:basic-auth [(:arvo-user env) @pwd]
                        :as :json}))))
