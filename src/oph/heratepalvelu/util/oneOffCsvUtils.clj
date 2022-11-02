(ns oph.heratepalvelu.util.oneOffCsvUtils
  (:require [oph.heratepalvelu.common :as c]
            [clj-http.client :as client]
            [clojure.data.json :as json]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [environ.core :refer [env]])
  (:import (java.time LocalDate)
           (clojure.lang ExceptionInfo)))

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
  (Thread/sleep 300)
  (try (get-opiskeluoikeus oid)
       (catch ExceptionInfo e
         (when-not (and (:status (ex-data e))
                        (= 404 (:status (ex-data e))))
           (throw e)))))

(defn- file-to-vec
  [file]
  (with-open [rdr (io/reader file)]
    (vec (line-seq rdr))))

(defn- match-item
  [item]
  (and (= (get-in item [:rahoituskausi :S]) "2022-2023")
       (not (nil? (get-in item [:tunnus :S])))
       (or (nil? (get-in item [:rahoitusryhma :S]))
           (empty? (get-in item [:rahoitusryhma :S])))))

(defn- simple-item
  [matching-item]
  (hash-map :opiskeluoikeus_oid (get-in matching-item [:opiskeluoikeus_oid :S])
            :tunnus (get-in matching-item [:tunnus :S])
            :herate (get-in matching-item [:jakso_loppupvm :S])))

(def matching (let [dir (:tmp-dir env)
                    json-files (filter #(s/ends-with? % ".json")
                                       (seq (.list (io/file dir))))
                    items (map #(:Item (json/read-str % :key-fn keyword))
                               (flatten (map #(file-to-vec (s/join "/" [dir %]))
                                             json-files)))
                    matching-items (filter match-item items)]
                (map simple-item matching-items)))

(defn- resolve-rahoitusryhma
  [item]
  (let [opiskeluoikeus (get-opiskeluoikeus-catch-404
                         (:opiskeluoikeus_oid item))]
    (c/get-rahoitusryhma opiskeluoikeus (LocalDate/parse (:herate item)))))

(defn- write-csv-row
  [item w]
  (.write w
          (str (:tunnus item) "," (resolve-rahoitusryhma item) "\n"))
  (.flush w))

(defn generate-csv
  "Tämä funktio ajetaan paikallisesti lein replissä. Funktio odottaa .lein-env
  -tiedostossa olevan seuraavat arvot: koski-url, koski-user, koski-pwd, tmp-dir
  ja output-csv-filename. Laita S3:sta haetut jaksotunnus-taulun json-tiedostot
  määriteltyyn tmp-hakemistoon. CSV-tiedosto muodostuu myös samaan
  tmp-hakemistoon."
  ([amount]
   (with-open [w (io/writer (str (:tmp-dir env) "/" (:output-csv-filename env))
                            :append true)]
     (doall
       (map #(write-csv-row % w)
            (take amount matching)))))
  ([]
   (generate-csv 1)))
