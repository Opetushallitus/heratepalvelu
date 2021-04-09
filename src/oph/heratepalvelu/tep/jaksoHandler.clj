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
            [clj-time.core :as t]
            [clj-time.coerce :as coerce]
            [oph.heratepalvelu.external.ehoks :as ehoks])
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
             {:tyyppi                 (s/conditional not-empty s/Str)
              :alkupvm                (s/conditional not-empty s/Str)
              :loppupvm               (s/conditional not-empty s/Str)
              :hoks-id                s/Num
              :opiskeluoikeus-oid     (s/conditional not-empty s/Str)
              :oppija-oid             (s/conditional not-empty s/Str)
              :hankkimistapa-id       s/Num
              :hankkimistapa-tyyppi   (s/conditional not-empty s/Str)
              :tutkinnonosa-id        s/Num
              :tutkinnonosa-koodi     (s/maybe s/Str)
              :tutkinnonosa-nimi      (s/maybe s/Str)
              :tyopaikan-nimi         (s/conditional not-empty s/Str)
              :tyopaikan-ytunnus      (s/conditional not-empty s/Str)
              :tyopaikkaohjaaja-email (s/maybe s/Str)
              :tyopaikkaohjaaja-nimi  (s/conditional not-empty s/Str)})

(def tep-herate-checker
  (s/checker tep-herate-schema))

(defn check-duplicate-hankkimistapa [id]
  (if (empty? (ddb/get-item {:hankkimistapa-id {:n id}}
                            (:jaksotunnus-table env)))
    true
    (log/warn "Osaamisenhankkimistapa id:llä " id "on jo käsitelty.")))

(defn save-jaksotunnus [herate opiskeluoikeus koulutustoimija]
  (let [tapa-id (:hankkimistapa-id herate)]
    (when (check-duplicate-hankkimistapa tapa-id)
      (try
        (let [request-id (c/generate-uuid)
              niputuspvm (c/next-niputus-date (:loppupvm herate))
              suoritus   (c/get-suoritus opiskeluoikeus)
              tutkinto   (get-in
                           suoritus
                           [:koulutusmoduuli
                            :tunniste
                            :koodiarvo])
              arvo-resp  (arvo/get-jaksotunnus
                           (arvo/build-jaksotunnus-request-body
                             herate
                             opiskeluoikeus
                             request-id
                             koulutustoimija
                             suoritus
                             (coerce/to-sql-date niputuspvm)))
              tunnus (:tunnus arvo-resp)
              db-data {:hankkimistapa-id     [:n tapa-id]
                       :hankkimistapa-tyyppi [:s (:hankkimistapa-tyyppi herate)]
                       :tyopaikan-nimi       [:s (:tyopaikan-nimi herate)]
                       :tyopaikan-ytunnus    [:s (:tyopaikan-ytunnus herate)]
                       :tunnus               [:s tunnus]
                       :ohjaaja-nimi         [:s (:tyopaikkaohjaaja-nimi herate)]
                       :jakso-alkupvm        [:s (:alkupvm herate)]
                       :jakso-loppupvm       [:s (:loppupvm herate)]
                       :request-id           [:s request-id]
                       :tutkinto             [:s tutkinto]
                       :oppilaitos           [:s (:oid (:oppilaitos opiskeluoikeus))]
                       :hoks-id              [:n (:hoks-id herate)]
                       :opiskeluoikeus-oid   [:s (:oid opiskeluoikeus)]
                       :oppija-oid           [:s (:oppija-oid herate)]
                       :koulutustoimija      [:s koulutustoimija]
                       :niputuspvm           [:s (coerce/to-sql-date niputuspvm)]
                       :viimeinen-vastauspvm [:s (coerce/to-sql-date
                                                   (t/plus
                                                     niputuspvm
                                                     (t/days 60)))]
                       :rahoituskausi        [:s (c/kausi (:loppupvm herate))]
                       :tallennuspvm         [:s (str (t/today))]
                       :tutkinnonosa-tyyppi  [:s (:tyyppi herate)]
                       :tutkinnonosa-id      [:n (:tutkinnonosa-id herate)]}]
          (try
            (ddb/put-item
              (cond-> db-data
                      (not-empty (:tyopaikkaohjaaja-email herate))
                      (assoc :ohjaaja-email (:tyopaikkaohjaaja-email herate))
                      (not-empty (:tutkinnonosa-koodi herate))
                      (assoc :tutkinnonosa-koodi (:tutkinnonosa-koodi herate)))
              {:cond-expr (str "attribute_not_exists(hankkimistapa-id)")}
              (:jaksotunnus-table env))
            (ddb/put-item
              {:ohjaaja-ytunnus-kj-tutkinto
                                            [:s (str
                                                  (:tyopaikkaohjaaja-nimi herate) "/"
                                                  (:tyopaikan-ytunnus herate) "/"
                                                  koulutustoimija "/"
                                                  tutkinto)]
               :ohjaaja                     [:s (:tyopaikkaohjaaja-nimi herate)]
               :ytunnus                     [:s (:tyopaikan-ytunnus herate)]
               :koulutuksenjarjestaja       [:s koulutustoimija]
               :oppilaitos                  [:s (:oid (:oppilaitos opiskeluoikeus))]
               :tutkinto                    [:s tutkinto]
               :niputuspvm                  [:s niputuspvm]}
              {} (:nippu-table env))
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
                ; (c/check-organisaatio-whitelist? koulutustoimija)
                (c/check-opiskeluoikeus-suoritus-types? opiskeluoikeus)
                (c/check-sisaltyy-opiskeluoikeuteen? opiskeluoikeus))
              (save-jaksotunnus herate opiskeluoikeus koulutustoimija)))
          (ehoks/patch-osaamisenhankkimistapa-tep-kasitelty
            (:hankkimistapa-id herate)))
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
