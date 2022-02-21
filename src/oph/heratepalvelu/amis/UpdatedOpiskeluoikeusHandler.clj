(ns oph.heratepalvelu.amis.UpdatedOpiskeluoikeusHandler
  (:require
    [clj-time.coerce :as c]
    [clj-time.core :as t]
    [clj-time.format :as f]
    [clojure.tools.logging :as log]
    [environ.core :refer [env]]
    [oph.heratepalvelu.amis.AMISCommon :as ac]
    [oph.heratepalvelu.common :refer :all]
    [oph.heratepalvelu.db.dynamodb :as ddb]
    [oph.heratepalvelu.external.ehoks :refer [get-hoks-by-opiskeluoikeus]]
    [oph.heratepalvelu.external.koski :refer [get-updated-opiskeluoikeudet]]
    [oph.heratepalvelu.log.caller-log :refer :all]
    [schema.core :as s])
  (:import (clojure.lang ExceptionInfo)))

;; Hakee päivitettyjä opiskeluoikeuksia koskesta ja tallentaa niiden tiedot
;; tietokantaan.

(gen-class
  :name "oph.heratepalvelu.amis.UpdatedOpiskeluoikeusHandler"
  :methods [[^:static handleUpdatedOpiskeluoikeus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn parse-herate
  "Luo heräte-objektin HOKSista, kyselytyypistä ja alkupäivämäärästä."
  [hoks kyselytyyppi alkupvm]
  {:ehoks-id           (:id hoks)
   :kyselytyyppi       kyselytyyppi
   :opiskeluoikeus-oid (:opiskeluoikeus-oid hoks)
   :oppija-oid         (:oppija-oid hoks)
   :sahkoposti         (:sahkoposti hoks)
   :alkupvm            alkupvm})

(defn get-vahvistus-pvm
  "Palauttaa ensimmäisen hyväksyttävän suorituksen vahvistuspäivämäärä, jos
  sellainen on olemassa."
  [opiskeluoikeus]
  (if-let [vahvistus-pvm
           (reduce
             (fn [_ suoritus]
               (when
                 (check-suoritus-type? suoritus)
                 (reduced (get-in suoritus [:vahvistus :päivä]))))
             nil (:suoritukset opiskeluoikeus))]
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
                 (t/minutes 5))]
    (ddb/update-item
      {:key [:s "opiskeluoikeus-last-checked"]}
      {:update-expr     "SET #value = :value"
       :expr-attr-names {"#value" "value"}
       :expr-attr-vals {":value" [:s (str time-with-buffer)]}}
      (:metadata-table env))))

(defn update-last-page
  "Tallentaa viimeisen sivun numeron tietokantaan."
  [page]
  (ddb/update-item
    {:key [:s "opiskeluoikeus-last-page"]}
    {:update-expr     "SET #value = :value"
     :expr-attr-names {"#value" "value"}
     :expr-attr-vals {":value" [:s (str page)]}}
    (:metadata-table env)))

(defn get-kysely-type
  "Hakee opiskeluoikeuden suorituksen tyypin."
  [opiskeluoikeus]
  (let [tyyppi (get-in
                 (get-suoritus opiskeluoikeus)
                 [:tyyppi :koodiarvo])]
    (cond
      (= tyyppi "ammatillinentutkinto")
      "tutkinnon_suorittaneet"
      (= tyyppi "ammatillinentutkintoosittainen")
      "tutkinnon_osia_suorittaneet")))

(defn get-tila
  "Hakee opiskeluoikeuden tilan."
  [opiskeluoikeus vahvistus-pvm]
  (let [tilat (:opiskeluoikeusjaksot (:tila opiskeluoikeus))
        voimassa (reduce
                   (fn [res next]
                     (if (>= (compare vahvistus-pvm (:alku next)) 0)
                       next
                       (reduced res)))
                   (sort-by :alku tilat))]
    (:koodiarvo (:tila voimassa))))

(defn check-tila
  "Varmistaa, että opiskeluoikeuden tila on 'valmistunut' tai 'läsnä'."
  [opiskeluoikeus vahvistus-pvm]
  (let [tila (get-tila opiskeluoikeus vahvistus-pvm)]
    (if (or (= tila "valmistunut") (= tila "lasna"))
      true
      (do (log/info "Opiskeluoikeuden"
                    (:oid opiskeluoikeus)
                    "(vahvistuspäivämäärä: "
                    vahvistus-pvm
                    ") tila on"
                    tila
                    ". Odotettu arvo on 'valmistunut' tai 'läsnä'.")
          false))))

(defn -handleUpdatedOpiskeluoikeus
  "Hakee päivitettyjä opiskeluoikeuksia koskesta ja tallentaa niiden tiedot
  tietokantaan."
  [this event context]
  (log-caller-details-scheduled "handleUpdatedOpiskeluoikeus" event context)
  (let [start-time (System/currentTimeMillis)
        last-checked
        (:value (ddb/get-item
                  {:key [:s "opiskeluoikeus-last-checked"]}
                  (:metadata-table env)))
        last-page
        (Integer. (:value (ddb/get-item
                            {:key [:s "opiskeluoikeus-last-page"]}
                            (:metadata-table env))))]
    (log/info "Käsitellään" last-checked "jälkeen muuttuneet opiskeluoikeudet")
    (loop [opiskeluoikeudet (get-updated-opiskeluoikeudet
                              last-checked last-page)
           next-page (+ last-page 1)]
      (if (seq opiskeluoikeudet)
        (do (doseq [opiskeluoikeus opiskeluoikeudet]
              (let [koulutustoimija (get-koulutustoimija-oid opiskeluoikeus)
                    vahvistus-pvm (get-vahvistus-pvm opiskeluoikeus)]
                (when (and (some? vahvistus-pvm)
                           (check-valid-herate-date vahvistus-pvm)
                           (check-organisaatio-whitelist?
                             koulutustoimija
                             (date-string-to-timestamp
                               vahvistus-pvm))
                           (nil? (:sisältyyOpiskeluoikeuteen opiskeluoikeus))
                           (check-tila opiskeluoikeus vahvistus-pvm))
                  (if-let [hoks
                           (try
                             (get-hoks-by-opiskeluoikeus (:oid opiskeluoikeus))
                             (catch ExceptionInfo e
                               (if (= 404 (:status (ex-data e)))
                                 (log/warn "Opiskeluoikeudella"
                                           (:oid opiskeluoikeus)
                                           "ei HOKSia. "
                                           "Koulutustoimija: "
                                           koulutustoimija)
                                 (throw e))))]
                    (if (:osaamisen-hankkimisen-tarve hoks)
                      (ac/save-herate
                        (parse-herate
                          hoks
                          (get-kysely-type opiskeluoikeus)
                          vahvistus-pvm)
                        opiskeluoikeus
                        koulutustoimija)
                      (log/info
                        "Ei osaamisen hankkimisen tarvetta hoksissa " (:id hoks)))))))
            (log/info (str "Käsitelty " (count opiskeluoikeudet)
                           " opiskeluoikeutta, sivu " (- next-page 1)))
            (update-last-page next-page)
            (when (< 120000 (.getRemainingTimeInMillis context))
              (recur
                (get-updated-opiskeluoikeudet last-checked next-page)
                (+ next-page 1))))
        (do
          (update-last-page 0)
          (update-last-checked (c/from-long start-time)))))))
