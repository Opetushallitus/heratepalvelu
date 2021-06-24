(ns oph.heratepalvelu.external.arvo
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.external.http-client :as client]
            [oph.heratepalvelu.external.koski :as koski]
            [oph.heratepalvelu.external.organisaatio :as org]
            [oph.heratepalvelu.external.ehoks :as ehoks]
            [cheshire.core :refer [generate-string]]
            [clojure.string :as str]
            [oph.heratepalvelu.external.aws-ssm :as ssm]
            [clj-time.core :as t])
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

(defn get-osaamisalat [suoritus opiskeluoikeus-oid]
  (let [osaamisalat (filter
                      #(and
                         (or (nil? (:loppu %1))
                             (>= (compare (:loppu %1)
                                          (str (t/today)))
                                 0))
                         (or (nil? (:alku %1))
                             (<= (compare (:alku %1)
                                          (str (t/today)))
                                 0)))
                      (:osaamisala suoritus))]
    (if (not-empty osaamisalat)
      (map #(or (:koodiarvo (:osaamisala %1))
                (:koodiarvo %1))
           osaamisalat)
      (log/warn "Ei osaamisaloja opiskeluoikeudessa" opiskeluoikeus-oid))))

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
  {:vastaamisajan_alkupvm          (:alkupvm herate)
   :kyselyn_tyyppi                 (:kyselytyyppi herate)
   :tutkintotunnus                 (get-in suoritus [:koulutusmoduuli
                                                     :tunniste
                                                     :koodiarvo])
   :tutkinnon_suorituskieli        (str/lower-case
                                     (:koodiarvo (:suorituskieli suoritus)))
   :osaamisala                     (get-osaamisalat suoritus (:oid opiskeluoikeus))
   :koulutustoimija_oid            koulutustoimija
   :oppilaitos_oid                 (:oid (:oppilaitos opiskeluoikeus))
   :request_id                     request-id
   :toimipiste_oid                 (get-toimipiste suoritus)
   :hankintakoulutuksen_toteuttaja (get-hankintakoulutuksen-toteuttaja
                                     (:ehoks-id herate))})

(defn create-amis-kyselylinkki [data]
  (try
    (let [resp (client/post
                 (str (:arvo-url env) "vastauslinkki/v1")
                 {:content-type "application/json"
                  :body         (generate-string data)
                  :basic-auth   [(:arvo-user env) @pwd]
                  :as           :json})]
      (:body resp))
    (catch ExceptionInfo e
      (log/error e)
      (when-not (= 404 (:status (ex-data e)))
        (throw e)))))

(defn delete-amis-kyselylinkki [linkki]
  (let [tunnus (last (str/split linkki #"/"))]
    (client/delete (str (:arvo-url env) "vastauslinkki/v1/" tunnus)
                   {:basic-auth [(:arvo-user env) @pwd]})))

(defn get-kyselylinkki-status [linkki]
  (let [tunnus (last (str/split linkki #"/"))]
    (:body (client/get (str (:arvo-url env) "vastauslinkki/v1/status/" tunnus)
                       {:basic-auth [(:arvo-user env) @pwd]
                        :as         :json}))))

(defn patch-kyselylinkki-metadata [linkki tila]
  (try
    (let [tunnus (last (str/split linkki #"/"))]
      (:body (client/patch
               (str (:arvo-url env) "vastauslinkki/v1/" tunnus "/metatiedot")
               {:basic-auth   [(:arvo-user env) @pwd]
                :content-type "application/json"
                :body         (generate-string {:tila tila})
                :as           :json})))

    (catch ExceptionInfo e
      (log/error e)
      (when-not (= 404 (:status (ex-data e)))
        (throw e)))))

(defn build-jaksotunnus-request-body [herate
                                      kesto
                                      opiskeluoikeus
                                      request-id
                                      koulutustoimija
                                      suoritus
                                      niputuspvm]
  {:koulutustoimija_oid       koulutustoimija
   :tyonantaja                (:tyopaikan-ytunnus herate)
   :tyopaikka                 (:tyopaikan-nimi herate)
   :tutkintotunnus            (get-in
                                suoritus
                                [:koulutusmoduuli
                                 :tunniste
                                 :koodiarvo])
   :tutkinnon_osa             (when (:tutkinnonosa-koodi herate)
                                (last
                                  (str/split
                                    (:tutkinnonosa-koodi herate)
                                    #"_")))
   :paikallinen_tutkinnon_osa (:tutkinnonosa-nimi herate)
   :tutkintonimike            (map
                                :koodiarvo
                                (:tutkintonimike suoritus))
   :osaamisala                (get-osaamisalat suoritus (:oid opiskeluoikeus))
   :tyopaikkajakson_alkupvm   (:alkupvm herate)
   :tyopaikkajakson_loppupvm  (:loppupvm herate)
   :tyopaikkajakson_kesto     kesto
   :osa_aikaisuus             (:osa-aikaisuus herate)
   :sopimustyyppi             (last
                                (str/split
                                  (:hankkimistapa-tyyppi herate)
                                  #"_"))
   :vastaamisajan_alkupvm     niputuspvm
   ;:vastaamisajan_loppupvm
   :oppilaitos_oid            (:oid (:oppilaitos opiskeluoikeus))
   :toimipiste_oid            (get-toimipiste suoritus)
   :request_id                request-id})

(defn create-jaksotunnus [data]
  (try
    (let [resp (client/post
                 (str (:arvo-url env) "tyoelamapalaute/v1/vastaajatunnus")
                 {:content-type "application/json"
                  :body         (generate-string data)
                  :basic-auth   [(:arvo-user env) @pwd]
                  :as           :json})]
      resp)
    (catch ExceptionInfo e
      (log/error e)
      (when-not (= 404 (:status (ex-data e)))
        (throw e)))))

(defn delete-jaksotunnus [tunnus]
  (client/delete
    (str (:arvo-url env) "tyoelamapalaute/v1/vastaajatunnus/" tunnus)
    {:basic-auth   [(:arvo-user env) @pwd]}))

(defn build-niputus-request-body [tunniste
                                  nippu
                                  tunnukset
                                  request-id]
  {:tunniste            tunniste
   :koulutustoimija_oid (:koulutuksenjarjestaja nippu)
   :tutkintotunnus      (:tutkinto nippu)
   :tyonantaja          (:ytunnus nippu)
   :tyopaikka           (:tyopaikka nippu)
   :tunnukset           tunnukset
   :voimassa_alkupvm    (str (t/today))
   :request_id          request-id})

(defn create-nippu-kyselylinkki [data]
  (try
    (let [resp (client/post
                 (str (:arvo-url env) "tyoelamapalaute/v1/nippu")
                 {:content-type "application/json"
                  :body         (generate-string data)
                  :basic-auth   [(:arvo-user env) @pwd]
                  :as           :json})]
      (log/info resp)
      (:body resp))
    (catch ExceptionInfo e
      (log/error e)
      (when-not (= 404 (:status (ex-data e)))
        (throw e)))))

(defn delete-nippukyselylinkki [tunniste]
  (client/delete
    (str (:arvo-url env) "tyoelamapalaute/v1/nippu/" tunniste)
    {:basic-auth   [(:arvo-user env) @pwd]}))

(defn patch-nippulinkki-metadata [linkki tila]
  (try
    (let [tunniste (last (str/split linkki #"/"))]
      (:body (client/patch
               (str (:arvo-url env) "tyoelamapalaute/v1/nippu/" tunniste "/metatiedot")
               {:basic-auth   [(:arvo-user env) @pwd]
                :content-type "application/json"
                :body         (generate-string {:tila tila})
                :as           :json})))
    (catch ExceptionInfo e
      (log/error e)
      (when-not (= 404 (:status (ex-data e)))
        (throw e)))))
