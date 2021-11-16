(ns oph.heratepalvelu.tpk.tpkNiputusHandler
  (:require [clj-time.core :as t]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.log.caller-log :refer :all])
  (:import (clojure.lang ExceptionInfo)
           (com.amazonaws.services.lambda.runtime Context)
           (com.amazonaws.services.lambda.runtime.events ScheduledEvent)
           (java.time LocalDate)
           (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class :name "oph.heratepalvelu.tpk.tpkNiputusHandler"
           :methods [[^:static handleTpkNiputus [ScheduledEvent Context] void]])

(defn check-jakso? [jakso]
  (and (:koulutustoimija jakso)
       (:tyopaikan_nimi jakso)
       (:tyopaikan_ytunnus jakso)
       (or (not= (:hankkimistapa_tyyppi jakso) "oppisopimus")
           (not= (:oppisopimuksen_perusta jakso) "02"))))

(defn get-kausi-alkupvm [jakso]
  (let [jakso-loppupvm (c/to-date (:jakso_loppupvm jakso))]
    (str (.getYear jakso-loppupvm)
         (if (<= (.getMonthValue jakso-loppupvm) 6) "-01-01" "-07-01"))))

(defn get-kausi-loppupvm [jakso]
  (let [jakso-loppupvm (c/to-date (:jakso_loppupvm jakso))]
    (str (.getYear jakso-loppupvm)
         (if (<= (.getMonthValue jakso-loppupvm) 6) "-06-30" "-12-31"))))

(defn create-nippu-id [jakso]
  (str (c/normalize-string (:tyopaikan_nimi jakso)) "/"
       (:tyopaikan_ytunnus jakso) "/"
       (:koulutustoimija jakso) "/"
       (get-kausi-alkupvm jakso) "_"
       (get-kausi-loppupvm)))

;; TODO is this right?
(defn- get-existing-nippu [jakso]
  (try
    (ddb/get-item {:nippu-id [:s (create-nippu-id jakso)]}
                  {:tpk-nippu-table env})
    (catch AwsServiceException e
      nil)))

(defn get-next-vastaamisajan-alkupvm-date [jakso]
  (let [loppupvm (c/to-date (:jakso_loppupvm jakso))
        year (.getYear loppupvm)
        kausi-month (if (<= (.getMonthValue loppupvm) 6) 7 1)
        kausi-year (if (= kausi-month 1) (+ year 1) year)]
    (LocalDate/of kausi-year kausi-month 1)))

(defn create-nippu [jakso]
  (let [alkupvm (get-next-vastaamisajan-alkupvm-date jakso)
        loppupvm (.minusDays (.plusMonths alkupvm 2) 1)]
    {:nippu-id               (create-nippu-id jakso)
     :koulutustoimija_oid    (:koulutustoimija jakso)
     :tyonantaja             (:tyopaikan_nimi jakso)
     :tyopaikka              (:tyopaikan_ytunnus jakso)
     :vastaamisajan_alkupvm  (str alkupvm)
     :vastaamisajan_loppupvm (str loppupvm)
     :tiedonkeruu_alkupvm    (get-kausi-alkupvm jakso)
     :tiedonkeruu_loppupvm   (get-kausi-loppupvm jakso)}))


(defn- save-nippu [nippu]
  (try
    (ddb/put-item nippu {} (:tpk-nippu-table env))
    (catch AwsServiceException e
      (log/error "Virhe DynamoDB tallennuksessa (TPK):" e))))

(defn- get-kyselylinkki [nippu]
  (try
    (let [body (arvo/create-tpk-kyselylinkki (dissoc nippu :nippu-id))]
      (when body
        (:kyselylinkki body))) ;; TODO en ole varma tästä nimestä
    (catch ExceptionInfo e
      (log/error "Ei luonut kyselylinkkiä nipulle:" (:nippu-id nippu)))))

(defn- query-niputtamattomat []
  (ddb/query-items {:tpk-niputuspvm [:null]
                    :jakso_loppupvm [:le [:s (str (t/today))]]}
                   {:index "tpkNiputusIndex"
                    :limit 10}
                   (:jaksotunnus-table env)))

(defn -handleTpkNiputus [this event context]
  (log-caller-details-scheduled "handleTpkNiputus" event context)
  (loop [niputettavat (query-niputtamattomat)]
    (log/info "Käsitellään" (count niputettavat) "niputusta.")
    (when (seq niputettavat)
      (doseq [jakso niputettavat]
        (if (check-jakso? jakso)
          (let [existing-nippu (get-existing-nippu jakso)]
            (when (empty? existing-nippu)
              (let [nippu (create-nippu jakso)
                    kyselylinkki (get-kyselylinkki nippu)]
                (if kyselylinkki ;; TODO if save-nippu returns nil, delete kyselylinkki (or reverse?)
                  (do
                    (save-nippu (assoc nippu :kyselylinkki kyselylinkki))
                    (ddb/update-item
                      {:hankkimistapa_id [:n (:hankkimistapa_id jakso)]}
                      {:update-expr "SET #value = :value"
                       :expr-attr-names {"#value" "tpk-nipututspvm"}
                       :expr-attr-vals {":value" [:s (str (t/today))]}}
                      (:jaksotunnus-table env)))
                  (log/error "Kyselylinkkiä ei saatu Arvolta. Jakso:"
                             (:hankkimistapa_id jakso))))))
          (log/info "Jaksoa ei oteta mukaan:" (:hankkimistapa_id jakso))))
      (when (< 120000 (.getRemainingTimeInMillis context))
        (recur (query-niputtamattomat))))))
