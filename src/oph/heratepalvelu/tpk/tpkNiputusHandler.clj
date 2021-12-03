(ns oph.heratepalvelu.tpk.tpkNiputusHandler
  (:require [clj-time.core :as t]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.log.caller-log :refer :all])
  (:import (clojure.lang ExceptionInfo)
           (java.time LocalDate)
           (software.amazon.awssdk.awscore.exception AwsServiceException)
           (software.amazon.awssdk.services.dynamodb.model ScanRequest
                                                           AttributeValue)))

(gen-class :name "oph.heratepalvelu.tpk.tpkNiputusHandler"
           :methods [[^:static handleTpkNiputus
                      [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
                       com.amazonaws.services.lambda.runtime.Context] void]])

(defn check-jakso? [jakso]
  (and (:koulutustoimija jakso)
       (:tyopaikan_nimi jakso)
       (:tyopaikan_ytunnus jakso)
       (:jakso_loppupvm jakso)
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
       (get-kausi-loppupvm jakso)))

(defn- get-existing-nippu [jakso]
  (try
    (ddb/get-item {:nippu-id [:s (create-nippu-id jakso)]}
                  (:tpk-nippu-table env))
    (catch AwsServiceException e
      nil)))

(defn get-next-vastaamisajan-alkupvm-date [jakso]
  (let [loppupvm (c/to-date (:jakso_loppupvm jakso))
        year (.getYear loppupvm)
        kausi-month (if (<= (.getMonthValue loppupvm) 6) 7 1)
        kausi-year (if (= kausi-month 1) (+ year 1) year)]
    (LocalDate/of kausi-year kausi-month 1)))

(defn create-nippu [jakso request-id]
  (let [alkupvm (get-next-vastaamisajan-alkupvm-date jakso)
        loppupvm (.minusDays (.plusMonths alkupvm 2) 1)
        kausi-alkupvm (get-kausi-alkupvm jakso)
        kausi-loppupvm (get-kausi-loppupvm jakso)]
    {:nippu-id                    (create-nippu-id jakso)
     :tyopaikan-nimi              (:tyopaikan_nimi jakso)
     :tyopaikan-nimi-normalisoitu (c/normalize-string (:tyopaikan_nimi jakso))
     :vastaamisajan-alkupvm       "2022-01-15" ; (str alkupvm) ; TODO vastaamisaika alkaa 2022-01-15 vuoden 2022 keväällä. Muutetaan takaisin 1.3.2022.
     :vastaamisajan-loppupvm      (str loppupvm)
     :tyopaikan-ytunnus           (:tyopaikan_ytunnus jakso)
     :koulutustoimija-oid         (:koulutustoimija jakso)
     :tiedonkeruu-alkupvm         kausi-alkupvm
     :tiedonkeruu-loppupvm        kausi-loppupvm
     :niputuspvm                  (str (t/today))
     :request-id                  request-id}))


(defn- save-nippu [nippu]
  (try
    (ddb/put-item
      (reduce #(assoc %1 (first %2) [:s (second %2)]) {} (seq nippu))
      {}
      (:tpk-nippu-table env))
    (catch AwsServiceException e
      (log/error "Virhe DynamoDB tallennuksessa (TPK):" e))))

(defn- make-arvo-request [nippu]
  (try
    (arvo/create-tpk-kyselylinkki (arvo/build-tpk-request-body nippu))
    (catch ExceptionInfo e
      (log/error "Ei luonut kyselylinkkiä nipulle:" (:nippu-id nippu)))))

(defn- query-niputtamattomat [exclusive-start-key]
  (let [req (-> (ScanRequest/builder)
                (.filterExpression "#tpkNpvm = :tpkNpvm AND #jl <= :jl")
                (cond->
                  exclusive-start-key
                  (.exclusiveStartKey exclusive-start-key))
                (.expressionAttributeNames
                  {"#tpkNpvm" "tpk-niputuspvm"
                   "#jl"      "jakso_loppupvm"})
                (.expressionAttributeValues
                  {":tpkNpvm" (.build (.s (AttributeValue/builder)
                                          "ei_maaritelty"))
                   ":jl"      (.build (.s (AttributeValue/builder)
                                          (str (t/today))))})
                (.tableName (:jaksotunnus-table env))
                (.build))
        response (.scan ddb/ddb-client req)]
    (log/info "TPK Funktion scan" (count (.items response)))
    response))

(defn -handleTpkNiputus [this event context]
  (log-caller-details-scheduled "handleTpkNiputus" event context)
  (loop [niputettavat (query-niputtamattomat nil)]
    (doseq [jakso (map ddb/map-attribute-values-to-vals (.items niputettavat))]
      (if (check-jakso? jakso)
        (let [existing-nippu (get-existing-nippu jakso)]
          (if (empty? existing-nippu)
            (let [request-id (c/generate-uuid)
                  nippu (create-nippu jakso request-id)
                  arvo-resp (make-arvo-request nippu)]
              (if (some? (:kysely_linkki arvo-resp))
                (do
                  (save-nippu
                    (assoc nippu
                           :kyselylinkki      (:kysely_linkki arvo-resp)
                           :tunnus            (:tunnus arvo-resp)
                           :voimassa-loppupvm (:voimassa_loppupvm arvo-resp)))
                  (ddb/update-item
                    {:hankkimistapa_id [:n (:hankkimistapa_id jakso)]}
                    {:update-expr "SET #value = :value"
                     :expr-attr-names {"#value" "tpk-niputuspvm"}
                     :expr-attr-vals {":value" [:s (:niputuspvm nippu)]}}
                    (:jaksotunnus-table env)))
                (log/error "Kyselylinkkiä ei saatu Arvolta. Jakso:"
                           (:hankkimistapa_id jakso))))
            (ddb/update-item
              {:hankkimistapa_id [:n (:hankkimistapa_id jakso)]}
              {:update-expr "SET #value = :value"
               :expr-attr-names {"#value" "tpk-niputuspvm"}
               :expr-attr-vals {":value" [:s (:niputuspvm
                                               (first existing-nippu))]}}
              (:jaksotunnus-table env))))
        (ddb/update-item
          {:hankkimistapa_id [:n (:hankkimistapa_id jakso)]}
          {:update-expr "SET #value = :value"
           :expr-attr-names {"#value" "tpk-niputuspvm"}
           :expr-attr-vals {":value" [:s "ei_niputeta"]}}
          (:jaksotunnus-table env))))
    (when (and (< 120000 (.getRemainingTimeInMillis context))
               (.hasLastEvaluatedKey niputettavat))
      (recur (query-niputtamattomat (.lastEvaluatedKey niputettavat))))))
