(ns oph.heratepalvelu.util.poimintaUtils
  (:require [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.organisaatio :as org]
            [cheshire.core :refer [parse-string]]
            [clj-http.client :as client]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]])
  (:import (java.time LocalDate)
           (clojure.lang ExceptionInfo)
           (java.io BufferedReader StringReader)))

(defn- koski-get
  [uri-path options]
  (client/get (str (:koski-url env) uri-path)
              (merge {:basic-auth [(:koski-user env) (:koski-pwd env)]}
                     options)))

(defn- get-opiskeluoikeus
  [oid]
  (:body (koski-get (str "/opiskeluoikeus/" oid) {:as :json})))

(defn- get-opiskeluoikeus-catch-404
  [oid]
  ; hidastetaan koski-palveluun kohdistuvaa hetkellistä kuormitusta
  (Thread/sleep 200)
  (println (str "Kutsutaan Koskea, opiskeluoikeus-oid: " oid))
  (try (get-opiskeluoikeus oid)
    (catch ExceptionInfo e
      (when-not (and (:status (ex-data e))
                     (= 404 (:status (ex-data e))))
        (throw e)))))

(defn get-organisaatio
  "Hakee organisaation OID:n perusteella."
  [oid]
  (try
    (:body (client/get (str (:organisaatio-url env) oid) {:as :json}))
    (catch ExceptionInfo e
      (do (log/error "Virhe hausta organisaatiopalvelusta:" e)
        (throw e)))))

(defn check-terminaalitilaiset
  [path-to-file]
  (let [content (slurp path-to-file)
        json-content (json/read-str content
                                    :key-fn keyword)
        items (:Items json-content)]
    (spit "out.csv" "kyselytyyppi;ehoks-id;koulutustoimija-oid;koulutustoimija-nimi;oppilaitos-oid;oppilaitos-nimi;vastausaika-alku;vastausaika-loppu;lahetystila;kyselylinkki;oppija-oid\n")
    (doseq [item items]
      (let [heratepvm (get-in item [:heratepvm :S])
            opiskeluoikeus-oid (get-in item [:opiskeluoikeus-oid :S])
            opiskeluoikeus (get-opiskeluoikeus-catch-404 opiskeluoikeus-oid)]
        (when (c/terminaalitilassa? opiskeluoikeus heratepvm)
          (let [kyselytyyppi (get-in item [:kyselytyyppi :S])
                ehoks-id (get-in item [:ehoks-id :N])
                koulutustoimija-oid (get-in item [:koulutustoimija :S])
                koulutustoimija-nimi (get-in (get-organisaatio koulutustoimija-oid) [:nimi :fi])
                oppilaitos-oid (get-in item [:oppilaitos :S])
                oppilaitos-nimi (get-in (get-organisaatio oppilaitos-oid) [:nimi :fi])
                vastausaika-alku (get-in item [:alkupvm :S])
                vastausaika-loppu (get-in item [:voimassa-loppupvm :S])
                lahetystila (get-in item [:lahetystila :S])
                kyselylinkki (get-in item [:kyselylinkki :S])
                oppija-oid (get-in item [:oppija-oid :S])]
            (spit "out.csv" (str (s/join ";" [kyselytyyppi
                                              ehoks-id
                                              koulutustoimija-oid
                                              koulutustoimija-nimi
                                              oppilaitos-oid
                                              oppilaitos-nimi
                                              vastausaika-alku
                                              vastausaika-loppu
                                              lahetystila
                                              kyselylinkki
                                              oppija-oid]) "\n") :append true)))))))

(defn enrich-csv-with-new-fields
  [path-to-json-file path-to-csv-file]
  (let [content (slurp path-to-json-file)
        json-content (json/read-str content
                                    :key-fn keyword)
        items (map #(select-keys % [:heratepvm :opiskeluoikeus-oid :kyselylinkki]) (:Items json-content))
        csv (slurp path-to-csv-file)
        all-lines (s/split-lines csv)
        headers (first all-lines)
        csv-lines (drop 1 all-lines)]
    (spit "enriched-out.csv" (str headers ";heratepvm;opiskeluoikeus-oid\n"))
    (doseq [line csv-lines]
      (let [kyselylinkki (second (reverse (s/split line #";")))
            item (first (filter #(= (get-in % [:kyselylinkki :S]) kyselylinkki) items))
            heratepvm (get-in item [:heratepvm :S])
            opiskeluoikeus-oid (get-in item [:opiskeluoikeus-oid :S])]
        (spit "enriched-out.csv" (str line ";" heratepvm ";" opiskeluoikeus-oid "\n") :append true)))))

(def aloituskysely-terminaalitilat
  #{"mitatoity"})

(def päättökysely-terminaalitilat
  #{"eronnut" "katsotaaneronneeksi" "mitatoity" "peruutettu"})

(defn check-terminaalitilassa-with-tila
  [opiskeluoikeus loppupvm kyselytyyppi]
  (let [jakso (c/get-opiskeluoikeusjakso-for-date
               opiskeluoikeus loppupvm :one-day-offset)
        tila (get-in jakso [:tila :koodiarvo])
        terminaalitilat (if (= kyselytyyppi "aloittaneet")
                          aloituskysely-terminaalitilat
                          päättökysely-terminaalitilat)]
    (if (terminaalitilat tila)
      (do
        (log/warn "Opiskeluoikeus"
                  (:oid opiskeluoikeus)
                  "terminaalitilassa"
                  tila
                  "kyselytyyppi"
                  kyselytyyppi)
        {:terminaalitilassa true :tila tila})
      {:terminaalitilassa false :tila tila})))

(defn check-terminaalitilaiset-v2
  [path-to-csv-file]
  (let [csv (slurp path-to-csv-file)
        all-lines (s/split-lines csv)
        headers (first all-lines)
        csv-lines (drop 1 all-lines)]
    (spit "with-tila-out.csv" (str headers ";opiskeluoikeus-tila\n"))
    (doseq [line csv-lines]
      (let [splitted-line (s/split line #";")
            opiskeluoikeus-oid (last splitted-line)
            heratepvm (second (reverse splitted-line))
            kyselytyyppi (first splitted-line)
            opiskeluoikeus (get-opiskeluoikeus-catch-404 opiskeluoikeus-oid)
            {:keys [terminaalitilassa tila]}
            (check-terminaalitilassa-with-tila opiskeluoikeus heratepvm kyselytyyppi)]
        (when terminaalitilassa
          (spit "with-tila-out.csv" (str line ";" tila "\n") :append true))))))
