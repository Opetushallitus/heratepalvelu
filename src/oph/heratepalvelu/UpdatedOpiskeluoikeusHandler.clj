(ns oph.heratepalvelu.UpdatedOpiskeluoikeusHandler
  (:require
    [oph.heratepalvelu.external.koski :refer [get-updated-opiskeluoikeudet]]
    [oph.heratepalvelu.external.ehoks :refer [get-hoks-by-opiskeluoikeus]]
    [oph.heratepalvelu.db.dynamodb :as ddb]
    [oph.heratepalvelu.common :refer :all]
    [environ.core :refer [env]]
    [clojure.tools.logging :as log]
    [schema.core :as s]
    [clj-time.format :as f]
    [clj-time.coerce :as c])
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

(defn date-string-to-timestamp [date]
  (c/to-long (f/parse (:date f/formatters)
                      date)))

(defn get-vahvistus-pvm [opiskeluoikeus]
  (if-let [vahvistus-pvm (-> (:suoritukset opiskeluoikeus)
                             (seq)
                             (first)
                             (:vahvistus)
                             (:päivä))]
    vahvistus-pvm
    (log/warn "Opiskeluoikeudessa" (:oid opiskeluoikeus)
              "ei vahvistus päivämäärää")))

(defn update-last-checked [datetime]
  (ddb/update-item
    {:key [:s "opiskeluoikeus-last-checked"]}
    {:update-expr     "SET #value = :value"
     :expr-attr-names {"#value" "value"}
     :expr-attr-vals {":value" [:s datetime]}}
    (:metadata-table env)))

(defn -handleUpdatedOpiskeluoikeus [this event context]
  (let [last-checked
        (:value (ddb/get-item
                  {:key [:s "opiskeluoikeus-last-checked"]}
                  (:metadata-table env)))]
    (log/info "Haetaan" last-checked "jälkeen muuttuneet opiskeluoikeudet")
    (loop [opiskeluoikeudet (get-updated-opiskeluoikeudet
                              last-checked)]
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
                (let [herate
                      (parse-herate
                        hoks
                        (cond
                          (= (get-in suoritus [:tyyppi :koodiarvo])
                             "ammatillinentutkinto")
                          "tutkinnon_suorittaneet"
                          (= (get-in suoritus [:tyyppi :koodiarvo])
                             "ammatillinentutkintoosittainen")
                          "tutkinnon_osia_suorittaneet")
                        vahvistus-pvm)]
                  (if (nil? (s/check herate-schema herate))
                    (try
                      (save-herate herate opiskeluoikeus)
                      (catch Exception e
                        (update-last-checked
                          (:aikaleima opiskeluoikeus))
                        (throw e)))
                    (log/error (s/check herate-schema hoks))))))))
        (update-last-checked (:aikaleima
                               (last opiskeluoikeudet)))
        (when (> 30000 (.getRemainingTimeInMillis context))
          ;(recur
          ;       (get-updated-opiskeluoikeudet
          ;         (timestamp-to-datetime-string
          ;           starting-time)))
          )))))