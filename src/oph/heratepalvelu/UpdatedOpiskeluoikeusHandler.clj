(ns oph.heratepalvelu.UpdatedOpiskeluoikeusHandler
  (:require
    [oph.heratepalvelu.external.koski :refer [get-updated-opiskeluoikeudet]]
    [oph.heratepalvelu.external.ehoks :refer [get-hoks-by-opiskeluoikeus]]
    [oph.heratepalvelu.db.dynamodb :as ddb]
    [oph.heratepalvelu.common :refer :all]
    [oph.heratepalvelu.log.caller-log :refer :all]
    [environ.core :refer [env]]
    [clojure.tools.logging :as log]
    [schema.core :as s]
    [clj-time.format :as f]
    [clj-time.coerce :as c]
    [clj-time.core :as t])
  (:import (clojure.lang ExceptionInfo)))

(gen-class
  :name "oph.heratepalvelu.UpdatedOpiskeluoikeusHandler"
  :methods [[^:static handleUpdatedOpiskeluoikeus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn parse-herate [hoks kyselytyyppi alkupvm]
  {:ehoks-id           (:id hoks)
   :kyselytyyppi       kyselytyyppi
   :opiskeluoikeus-oid (:opiskeluoikeus-oid hoks)
   :oppija-oid         (:oppija-oid hoks)
   :sahkoposti         (:sahkoposti hoks)
   :alkupvm            alkupvm})

(defn date-string-to-timestamp
  ([date-str fmt]
   (c/to-long (f/parse (fmt f/formatters)
                       date-str)))
  ([date-str]
   (date-string-to-timestamp date-str :date)))

(defn get-vahvistus-pvm [opiskeluoikeus]
  (if-let [vahvistus-pvm (-> (:suoritukset opiskeluoikeus)
                             (seq)
                             (first)
                             (:vahvistus)
                             (:päivä))]
    vahvistus-pvm
    (log/warn "Opiskeluoikeudessa" (:oid opiskeluoikeus)
              "ei vahvistus päivämäärää")))

(defn update-last-checked
  "Opiskeluoikeuksien aikaleimat eivät ole tarkkoja, vaan transaktion
   aloituksen aikoja, joten muutoksien hakuhetken jälkeen tietokantaan voi
   tallentua aikaleimoja menneisyyteen. Hakemalla 1 min bufferilla
   varmistetaan että kaikki muutokset käsitellään vähintään 1 kerran."
  [datetime]
  (let [time-with-buffer
        (t/minus datetime
                 (t/minutes 1))]
    (ddb/update-item
      {:key [:s "opiskeluoikeus-last-checked"]}
      {:update-expr     "SET #value = :value"
       :expr-attr-names {"#value" "value"}
       :expr-attr-vals {":value" [:s (str time-with-buffer)]}}
      (:metadata-table env))))

(defn get-kysely-type [suoritus]
  (cond
    (= (get-in suoritus [:tyyppi :koodiarvo])
       "ammatillinentutkinto")
    "tutkinnon_suorittaneet"
    (= (get-in suoritus [:tyyppi :koodiarvo])
       "ammatillinentutkintoosittainen")
    "tutkinnon_osia_suorittaneet"))

(defn -handleUpdatedOpiskeluoikeus [this event context]
  (log-caller-details "handleUpdatedOpiskeluoikeus" event context)
  (let [start-time (System/currentTimeMillis)
        last-checked
        (:value (ddb/get-item
                  {:key [:s "opiskeluoikeus-last-checked"]}
                  (:metadata-table env)))]
    (log/info "Haetaan" last-checked "jälkeen muuttuneet opiskeluoikeudet")
    (loop [opiskeluoikeudet (get-updated-opiskeluoikeudet
                              last-checked 0)
           next-page 1]
      (when (seq opiskeluoikeudet)
        (doseq [opiskeluoikeus opiskeluoikeudet]
          (let [koulustoimija (get-koulutustoimija-oid opiskeluoikeus)
                suoritus (first (seq (:suoritukset opiskeluoikeus)))
                vahvistus-pvm (get-vahvistus-pvm opiskeluoikeus)]
            (when (and (some? vahvistus-pvm)
                       (check-suoritus-type? suoritus)
                       (check-organisaatio-whitelist?
                         koulustoimija
                         (date-string-to-timestamp
                           vahvistus-pvm)))
              (if-let [hoks
                       (try
                         (get-hoks-by-opiskeluoikeus (:oid opiskeluoikeus))
                         (catch ExceptionInfo e
                           (if (= 404 (:status (ex-data e)))
                             (log/warn "Opiskeluoikeudella"
                                       (:oid opiskeluoikeus)
                                       "ei HOKSia")
                             (throw e))))]
                (save-herate
                  (parse-herate
                    hoks
                    (get-kysely-type suoritus)
                    vahvistus-pvm)
                  opiskeluoikeus)))))
        (when (> 30000 (.getRemainingTimeInMillis context))
          (recur
            (get-updated-opiskeluoikeudet last-checked next-page)
            (+ next-page 1)))))
    (update-last-checked (c/from-long start-time))))
