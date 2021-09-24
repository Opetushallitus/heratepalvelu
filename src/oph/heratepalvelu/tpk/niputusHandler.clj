(ns oph.heratepalvelu.tpk.niputusHandler
  (:require [clj-time.core :as t]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.log.caller-log :refer :all])
  (:import (com.amazonaws.services.lambda.runtime Context)
           (com.amazonaws.services.lambda.runtime.events ScheduledEvent)
           (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class :name "oph.heratepalvelu.tpk.tpkNiputusHandler"
           :methods [[^:static handleTpkNiputus [ScheduledEvent Context] void]])

(defn check-jakso? [jakso]
    ;; TODO palauttaa true, jos jaksossa on tarvittavia tietoja
  )

(defn create-nippu-id [jakso]
  (let [loppupvm (c/to-date (:jakso_loppupvm jakso))]
    (str (c/normalize-string (:tyopaikan_nimi jakso)) "/"
         (:koulutustoimija jakso) "/"
         (.getYear loppupvm) ;; kl/sl = kevät/syksy -lukukausi
         (if (<= (.getMonthValue loppupvm) 6) "kl" "sl"))))

(defn- get-existing-nippu [jakso]
  (try
    (ddb/get-item {:nippu-id [:s (create-nippu-id jakso)]}
                  {:tpk-nippu-table env})
    (catch AwsServiceException e
      nil)))

(defn create-nippu [jakso]
  ;; TODO
  )

(defn- save-nippu [nippu]
  (try
    ;; TODO toimii nyt, kun emme päivitä olemassaolevia nippuja. Myöhemmin tämä
    ;; täytyy korvata update-item -funktiolla.
    (ddb/put-item nippu {} (:tpk-nippu-table env))
    (catch AwsServiceException e
      (log/error "Virhe DynamoDB tallennuksessa (TPK):" e))))

(defn- get-kyselylinkki [nippu]
  ;; TODO + error handling
  )

(defn- query-niputtamattomat []
  (ddb/query-items {:tpk-niputuspvm [:null]
                    :jakso_loppupvm [:le [:s (str (t/today))]]}
                   {:index "niputusIndex"
                    :limit 10}
                   (:jaksotunnus-table env)))

;; TODO suodata pois yrittäjien oppisopimukset
;; TODO Onko kaikki oppisopimukset yrittäjien? Todennäköisesti ei.
(defn -handleTpkNiputus [this event context]
  (log-caller-details-scheduled "handleTpkNiputus" event context)
  (loop [niputettavat (query-niputtamattomat)]
    (log/info "Käsitellään" (count niputettavat) "niputusta.")
    (when (seq niputettavat)
      (doseq [jakso niputettavat]
        (when (check-jakso? jakso)
          (let [existing-nippu (get-existing-nippu jakso)]
            (if existing-nippu
              (do) ;; TODO skip for now if nippu already exists
              (let [nippu (create-nippu jakso)
                    kyselylinkki (get-kyselylinkki nippu)]
                (when kyselylinkki ;; TODO if save-nippu returns nil, delete kyselylinkki
                  (save-nippu (assoc nippu :kyselylinkki kyselylinkki))))))))
      (when (< 120000 (.getRemainingTimeInMillis context))
        (recur (query-niputtamattomat))))))
