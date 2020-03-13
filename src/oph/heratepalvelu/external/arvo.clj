(ns oph.heratepalvelu.external.arvo
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.external.http-client :as client]
            [oph.heratepalvelu.external.organisaatio :as org]
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

(defn build-arvo-request-body [herate
                               opiskeluoikeus
                               request-id
                               koulutustoimija]
  (let [suoritus (first (:suoritukset opiskeluoikeus))]
    {:vastaamisajan_alkupvm (:alkupvm herate)
     :kyselyn_tyyppi (:kyselytyyppi herate)
     :tutkintotunnus (:koodiarvo
                       (:tunniste
                         (:koulutusmoduuli suoritus)))
     :tutkinnon_suorituskieli (str/lower-case
                                (:koodiarvo (:suorituskieli suoritus)))
     :koulutustoimija_oid koulutustoimija
     :oppilaitos_oid (:oid (:oppilaitos opiskeluoikeus))
     :request_id request-id
     :toimipiste_oid                 (get-toimipiste suoritus)
     :hankintakoulutuksen_toteuttaja nil}))

(defn get-kyselylinkki [data]
  (try
    (let [resp (client/post
                 (:arvo-url env)
                 {:content-type "application/json"
                  :body (generate-string data)
                  :basic-auth [(:arvo-user env) @pwd]
                  :as :json})]
      (get-in resp [:body :kysely_linkki]))
    (catch ExceptionInfo e
      (log/error e)
      (when-not (= 404 (:status (ex-data e)))
        (throw e)))))

(defn deactivate-kyselylinkki [linkki]
  (let [tunnus (last (str/split linkki #"/"))]
    (client/delete (str (:arvo-url env) "/" tunnus)
                   {:basic-auth [(:arvo-user env) @pwd]})))
