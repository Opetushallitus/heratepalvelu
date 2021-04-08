(ns oph.heratepalvelu.tep.jaksoHandler
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.external.koski :as koski]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.log.caller-log :refer [log-caller-details-sqs]]
            [environ.core :refer [env]]
            [schema.core :as s]
            [clojure.string :as str]
            [clj-time.core :as t]
            [clj-time.coerce :as coerce])
  (:import (com.fasterxml.jackson.core JsonParseException)
           (clojure.lang ExceptionInfo)
           (software.amazon.awssdk.services.dynamodb.model ConditionalCheckFailedException)
           (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.tep.jaksoHandler"
  :methods [[^:static handleJaksoHerate
             [com.amazonaws.services.lambda.runtime.events.SQSEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(s/defschema tep-herate-schema
             {:tyyppi                 (s/conditional s/Str (s/pred not-empty))
              :alkupvm                (s/conditional s/Str (s/pred not-empty))
              :loppupvm               (s/conditional s/Str (s/pred not-empty))
              :hoks-id                s/Num
              :opiskeluoikeus-oid     (s/conditional s/Str (s/pred not-empty))
              :oppija-oid             (s/conditional s/Str (s/pred not-empty))
              :hankkimistapa-id       s/Num
              :hankkimistapa-tyyppi   (s/conditional s/Str (s/pred not-empty))
              :tutkinnonosa-id        s/Num
              :tutkinnonosa-koodi     s/Str
              :tyopaikan-nimi         (s/conditional s/Str (s/pred not-empty))
              :tyopaikan-ytunnus      (s/conditional s/Str (s/pred not-empty))
              (s/optional-key :tyopaikkaohjaaja-email)
                                      (s/conditional s/Str (s/pred not-empty))
              :tyopaikkaohjaaja-nimi  (s/conditional s/Str (s/pred not-empty))})

(def tep-herate-checker
  (s/checker tep-herate-schema))

(defn check-duplicate-hankkimistapa [id]
  (if (empty? (ddb/get-item {:hankkimistapa-id {:n id}}
                            (:jaksotunnus-table env)))
    true
    (log/warn "Osaamisenhankkimistapa id:llä " id "on jo käsitelty.")))

(defn- next-niputus-date [loppupvm]
  (let [[year month day] (map
                           #(Integer. %)
                           (str/split loppupvm #"-"))]
    (if (< day 16)
      (coerce/to-sql-date (t/nth-day-of-the-month year month 16))
      (coerce/to-sql-date (t/nth-day-of-the-month
                       (t/plus
                         (t/local-date year month day)
                         (t/months 1))
                       1)))))

(defn save-jaksotunnus [herate opiskeluoikeus koulutustoimija]
  (let [tapa-id (:hankkimistapa-id herate)]
    (when (check-duplicate-hankkimistapa tapa-id)
      (try
        (let [request-id (c/generate-uuid)
              niputuspvm (next-niputus-date (:loppupvm herate))
              arvo-resp (arvo/get-jaksotunnus
                          (arvo/build-jaksotunnus-request-body
                            herate
                            opiskeluoikeus
                            request-id
                            koulutustoimija
                            (c/get-suoritus opiskeluoikeus)
                            niputuspvm))
              tunnus (:tunnus arvo-resp)]
          (try
            (ddb/put-item
              {:hankkimistapa-id [:n tapa-id]
               :hankkimistapa-tyyppi [:s (:hankkimistapa-tyyppi herate)]
               :tyopaikan-nimi [:s (:tyopaikan-nimi herate)]
               :tyopaikan-ytunnus [:s (:tyopaikan-ytunnus herate)]
               :tunnus           [:s tunnus]
               :ohjaaja-email     [:s (:tyopaikkaohjaaja-email herate)]
               :ohjaaja-nimi    [:s (:tyopaikkaohjaaja-nimi herate)]
               :jakso-alkupvm         [:s (:alkupvm herate)]
               :jakso-loppupvm  [:s (:loppupvm herate)]
               :request-id      [:s request-id]
               :oppilaitos      [:s (:oid (:oppilaitos opiskeluoikeus))]
               :hoks-id            [:n (str (:hoks-id herate))]
               :opiskeluoikeus-oid  [:s (:oid opiskeluoikeus)]
               :oppija-oid          [:s (:oppija-oid herate)]
               :koulutustoimija     [:s koulutustoimija]
               :niputuspvm          [:s niputuspvm]
               :rahoituskausi       [:s (c/kausi (:loppupvm herate))]
               :tallennuspvm        [:s (str (t/today))]
               :tutkinnonosa-tyyppi [:s (:tyyppi herate)]
               :tutkinnonosa-id    [:s (:tutkinnonosa-id herate)]
               :tutkinnonosa-koodi [:s (:tutkinnonosa-koodi herate)]}
              {:cond-expr (str "attribute_not_exists(hankkimistapa-id)")}
              (:jaksotunnus-table env))
            (ddb/put-item
              {:email [:s (:tyopaikkaohjaaja-email herate)]
               :nimi [:s (:tyopaikkaohjaaja-nimi herate)]}
              {} (:ohjaaja-table env))
            (catch ConditionalCheckFailedException e
              (log/warn "Osaamisenhankkimistapa id:llä " tapa-id "on jo käsitelty.")
              ;(deactivate-kyselylinkki kyselylinkki)
              )
            (catch AwsServiceException e
              (log/error "Virhe tietokantaan tallennettaessa " tunnus " " request-id)
              ;(deactivate-kyselylinkki kyselylinkki)
              (throw e))))
        (catch Exception e
          ;(deactivate-kyselylinkki kyselylinkki)
          (log/error "Unknown error " e)
          (throw e))))))

(defn -handleJaksoHerate [this event context]
  (log-caller-details-sqs "handleTPOherate" event context)
  (let [messages (seq (.getRecords event))]
    (doseq [msg messages]
      (try
        (let [herate (parse-string (.getBody msg) true)
              opiskeluoikeus (koski/get-opiskeluoikeus (:opiskeluoikeus-oid herate))
              koulutustoimija (c/get-koulutustoimija-oid opiskeluoikeus)]
          (if (some? (tep-herate-checker herate))
            (log/error {:herate herate :msg (tep-herate-checker herate)})
            (when
              (and
                (c/check-organisaatio-whitelist? koulutustoimija)
                (c/check-opiskeluoikeus-suoritus-types? opiskeluoikeus))
              (save-jaksotunnus herate opiskeluoikeus koulutustoimija))))
        (catch JsonParseException e
          (log/error "Virhe viestin lukemisessa: " e))
        (catch ExceptionInfo e
          (if (and
                (:status (ex-data e))
                (< 399 (:status (ex-data e)))
                (> 500 (:status (ex-data e))))
            (if (= 404 (:status (ex-data e)))
              (log/error "Ei opiskeluoikeutta " (:opiskeluoikeus-oid
                                                  (parse-string (.getBody msg) true)))
              (log/error "Unhandled client error: " e))
            (do (log/error e)
                (throw e))))))))
