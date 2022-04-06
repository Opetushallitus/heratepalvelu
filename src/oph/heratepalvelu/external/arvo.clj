(ns oph.heratepalvelu.external.arvo
  "Wrapperit Arvon REST-rajapinnan ympäri."
  (:require [cheshire.core :refer [generate-string]]
            [clj-http.util :as util]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.aws-ssm :as ssm]
            [oph.heratepalvelu.external.ehoks :as ehoks]
            [oph.heratepalvelu.external.http-client :as client]
            [oph.heratepalvelu.external.koski :as koski]
            [oph.heratepalvelu.external.organisaatio :as org])
  (:import (clojure.lang ExceptionInfo)))

(def ^:private pwd
  "Arvon autentikoinnin salasana."
  (delay
    (ssm/get-secret (str "/" (:stage env) "/services/heratepalvelu/arvo-pwd"))))

(defn get-toimipiste
  "Palauttaa toimipisteen OID jos sen organisaatiotyyppi on toimipiste. Tämä
  tarkistetaan tekemällä request organisaatiopalveluun. Jos organisaatiotyyppi
  ei ole toimipiste, palauttaa nil."
  [suoritus]
  (let [oid (:oid (:toimipiste suoritus))
        org (org/get-organisaatio oid)
        org-tyypit (:tyypit org)]
    (when (some #{"organisaatiotyyppi_03"} org-tyypit)
      oid)))

(defn get-osaamisalat
  "Hakee voimassa olevat osaamisalat suorituksesta."
  [suoritus opiskeluoikeus-oid]
  (let [osaamisalat (filter
                      #(and
                         (or (nil? (:loppu %1))
                             (>= (compare (:loppu %1)
                                          (str (c/local-date-now)))
                                 0))
                         (or (nil? (:alku %1))
                             (<= (compare (:alku %1)
                                          (str (c/local-date-now)))
                                 0)))
                      (:osaamisala suoritus))]
    (if (not-empty osaamisalat)
      (map #(or (:koodiarvo (:osaamisala %1))
                (:koodiarvo %1))
           osaamisalat)
      (log/info "Ei osaamisaloja opiskeluoikeudessa" opiskeluoikeus-oid))))

(defn get-hankintakoulutuksen-toteuttaja
  "Hakee hankintakoulutuksen toteuttajan OID:n eHOKS-palvelusta ja Koskesta."
  [ehoks-id]
  (let [oids (ehoks/get-hankintakoulutus-oids ehoks-id)]
    (when (not-empty oids)
      (if (> (count oids) 1)
        (log/warn "Enemmän kuin yksi linkitetty opiskeluoikeus! HOKS-id:"
                  ehoks-id)
        (let [opiskeluoikeus (koski/get-opiskeluoikeus (first oids))
              toteuttaja-oid
              (get-in
                opiskeluoikeus
                [:koulutustoimija :oid])]
          (log/info "Hoks" ehoks-id ", hankintakoulutuksen toteuttaja:"
                    toteuttaja-oid)
          toteuttaja-oid)))))

(defn build-arvo-request-body
  "Luo AMISin Arvo-requestin dataobjektin."
  [herate opiskeluoikeus request-id koulutustoimija suoritus alkupvm loppupvm]
  {:vastaamisajan_alkupvm          alkupvm
   :heratepvm                      (:alkupvm herate)
   :vastaamisajan_loppupvm         loppupvm
   :kyselyn_tyyppi                 (:kyselytyyppi herate)
   :tutkintotunnus                 (get-in suoritus [:koulutusmoduuli
                                                     :tunniste
                                                     :koodiarvo])
   :tutkinnon_suorituskieli        (str/lower-case
                                     (:koodiarvo (:suorituskieli suoritus)))
   :osaamisala                     (get-osaamisalat suoritus
                                                    (:oid opiskeluoikeus))
   :koulutustoimija_oid            koulutustoimija
   :oppilaitos_oid                 (:oid (:oppilaitos opiskeluoikeus))
   :request_id                     request-id
   :toimipiste_oid                 (get-toimipiste suoritus)
   :hankintakoulutuksen_toteuttaja (get-hankintakoulutuksen-toteuttaja
                                     (:ehoks-id herate))})

(defn create-amis-kyselylinkki
  "Pyytää Arvolta uuden AMIS-kyselylinkin annettujen tietojen perusteella."
  [data]
  (try
    (let [resp (client/post
                 (str (:arvo-url env) "vastauslinkki/v1")
                 {:content-type "application/json"
                  :body         (generate-string data)
                  :basic-auth   [(:arvo-user env) @pwd]
                  :as           :json})]
      (:body resp))
    (catch ExceptionInfo e
      (log/error "request-id: " (:request_id data))
      (log/error e)
      (throw e))))

(defn create-amis-kyselylinkki-catch-404
  "Pyytää Arvolta uuden AMIS-kyselylinkin annettujen tietojen perusteella, ja
  palauttaa nil jos vastaus on 404-virhe."
  [data]
  (try
    (create-amis-kyselylinkki data)
    (catch ExceptionInfo e
      (when-not (= 404 (:status (ex-data e)))
        (throw e)))))

(defn delete-amis-kyselylinkki
  "Poistaa AMIS-kyselylinkin Arvosta."
  [linkki]
  (let [tunnus (last (str/split linkki #"/"))]
    (client/delete (str (:arvo-url env) "vastauslinkki/v1/" tunnus)
                   {:basic-auth [(:arvo-user env) @pwd]})))

(defn get-kyselylinkki-status
  "Hakee Arvolta AMIS-kyselylinkin tilan."
  [linkki]
  (let [tunnus (last (str/split linkki #"/"))]
    (:body (client/get (str (:arvo-url env) "vastauslinkki/v1/status/" tunnus)
                       {:basic-auth [(:arvo-user env) @pwd]
                        :as         :json}))))

(defn get-nippulinkki-status
  "Hakee TEP-kyselylinkin tilan."
  [linkki]
  (let [tunniste (last (str/split linkki #"/"))]
    (:body (client/get
             (str (:arvo-url env) "tyoelamapalaute/v1/status/" tunniste)
             {:basic-auth [(:arvo-user env) @pwd]
              :as         :json}))))

(defn patch-kyselylinkki-metadata
  "Päivittää AMIS-kyselinkin tilan Arvoon."
  [linkki tila]
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

(defn build-jaksotunnus-request-body
  "Luo dataobjektin TEP-jaksotunnuksen luomisrequestille."
  [herate
   tyopaikka-normalisoitu
   kesto
   opiskeluoikeus
   request-id
   koulutustoimija
   suoritus
   niputuspvm]
  {:koulutustoimija_oid       koulutustoimija
   :tyonantaja                (:tyopaikan-ytunnus herate)
   :tyopaikka                 (:tyopaikan-nimi herate)
   :tyopaikka_normalisoitu    tyopaikka-normalisoitu
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
   :oppisopimuksen_perusta    (when (:oppisopimuksen-perusta herate)
                                (last
                                  (str/split
                                    (:oppisopimuksen-perusta herate)
                                    #"_")))
   :vastaamisajan_alkupvm     niputuspvm
   :oppilaitos_oid            (:oid (:oppilaitos opiskeluoikeus))
   :toimipiste_oid            (get-toimipiste suoritus)
   :request_id                request-id})

(defn create-jaksotunnus
  "Pyytää Arvolta TEP-jaksotunnuksen."
  [data]
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

(defn delete-jaksotunnus
  "Poistaa TEP-jaksotunnuksen Arvosta."
  [tunnus]
  (client/delete
    (str (:arvo-url env) "tyoelamapalaute/v1/vastaajatunnus/" tunnus)
    {:basic-auth   [(:arvo-user env) @pwd]}))

(defn build-niputus-request-body
  "Luo dataobjektin TEP-nippulinkin luomisrequestille."
  [tunniste nippu tunnukset request-id]
  {:tunniste            tunniste
   :koulutustoimija_oid (:koulutuksenjarjestaja nippu)
   :tutkintotunnus      (:tutkinto nippu)
   :tyonantaja          (:ytunnus nippu)
   :tyopaikka           (:tyopaikka nippu)
   :tunnukset           tunnukset
   :voimassa_alkupvm    (str (c/local-date-now))
   :request_id          request-id})

(defn create-nippu-kyselylinkki
  "Pyytää TEP-kyselylinkin Arvolta."
  [data]
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

(defn delete-nippukyselylinkki
  "Poistaa TEP-kyselylinkin Arvosta."
  [tunniste]
  (client/delete
    (str (:arvo-url env) "tyoelamapalaute/v1/nippu/" (util/url-encode tunniste))
    {:basic-auth   [(:arvo-user env) @pwd]}))

(defn patch-nippulinkki
  "Päivittää TEP-kyselylinkin tiedot Arvoon."
  [linkki data]
  (try
    (let [tunniste (last (str/split linkki #"/"))]
      (:body (client/patch
               (str (:arvo-url env)
                    "tyoelamapalaute/v1/nippu/"
                    (util/url-encode tunniste))
               {:basic-auth   [(:arvo-user env) @pwd]
                :content-type "application/json"
                :body         (generate-string data)
                :as           :json})))
    (catch ExceptionInfo e
      (log/error "Virhe patch-nippulinkki -funktiossa")
      (log/error e)
      (throw e))))

(defn patch-vastaajatunnus
  "Päivittää TEP-jaksotunnuksen tiedot Arvoon."
  [tunnus data]
  (client/patch
    (str (:arvo-url env) "tyoelamapalaute/v1/vastaajatunnus/" tunnus)
    {:basic-auth   [(:arvo-user env) @pwd]
     :content-type "application/json"
     :body         (generate-string data)
     :as           :json}))

(defn build-tpk-request-body
  "Luo dataobjektin TPK-kyselylinkin luomisrequestille."
  [nippu]
  {:tyopaikka              (:tyopaikan-nimi nippu)
   :tyopaikka_normalisoitu (:tyopaikan-nimi-normalisoitu nippu)
   :vastaamisajan_alkupvm  (:vastaamisajan-alkupvm nippu)
   :tyonantaja             (:tyopaikan-ytunnus nippu)
   :koulutustoimija_oid    (:koulutustoimija-oid nippu)
   :tiedonkeruu_loppupvm   (:tiedonkeruu-loppupvm nippu)
   :tiedonkeruu_alkupvm    (:tiedonkeruu-alkupvm nippu)
   :request_id             (:request-id nippu)
   :vastaamisajan_loppupvm (:vastaamisajan-loppupvm nippu)})

(defn create-tpk-kyselylinkki
  "Pyytää TPK-kyselylinkin Arvolta annettujen tietojen perusteella."
  [data]
  (try
    (let [resp (client/post
                 (str (:arvo-url env)
                      "tyoelamapalaute/v1/tyopaikkakysely-tunnus")
                 {:content-type "application/json"
                  :body         (generate-string data)
                  :basic-auth   [(:arvo-user env) @pwd]
                  :as           :json})]
      (:body resp))
    (catch ExceptionInfo e
      (log/error e))))
