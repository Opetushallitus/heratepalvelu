(ns oph.heratepalvelu.amis.UpdatedOpiskeluoikeusHandler
  "Hakee päivitettyjä opiskeluoikeuksia koskesta ja tallentaa niiden tiedot
  tietokantaan."
  (:require [clj-time.coerce :as c]
            [clj-time.core :as t]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.amis.AMISCommon :as ac]
            [oph.heratepalvelu.common :refer :all]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.ehoks :as ehoks]
            [oph.heratepalvelu.external.koski :as k]
            [oph.heratepalvelu.log.caller-log :refer :all])
  (:import (clojure.lang ExceptionInfo)))

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
   :puhelinnumero      (:puhelinnumero hoks)
   :alkupvm            alkupvm})

(defn get-vahvistus-pvm
  "Palauttaa ensimmäisen hyväksyttävän suorituksen vahvistuspäivämäärän, jos
  sellainen on olemassa."
  [opiskeluoikeus]
  (or
    (some #(and (ammatillinen-tutkinto? %)
                (get-in % [:vahvistus :päivä]))
          (:suoritukset opiskeluoikeus))
    (log/info "Opiskeluoikeudessa" (:oid opiskeluoikeus)
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
      (create-update-item-options {:value [:s (str time-with-buffer)]})
      (:metadata-table env))))

(defn update-last-page
  "Tallentaa viimeisen sivun numeron tietokantaan."
  [page]
  (ddb/update-item
    {:key [:s "opiskeluoikeus-last-page"]}
    (create-update-item-options {:value [:s (str page)]})
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

(defn check-tila
  "Varmistaa, että opiskeluoikeuden tila on 'valmistunut' tai 'läsnä'."
  [opiskeluoikeus vahvistus-pvm]
  (let [tila (get-tila opiskeluoikeus vahvistus-pvm)]
    (if (or (= tila "valmistunut") (= tila "lasna"))
      true
      (do (log/info "Opiskeluoikeuden" (:oid opiskeluoikeus)
                    "(vahvistuspäivämäärä:" vahvistus-pvm ") tila on" tila
                    ". Odotettu arvo on 'valmistunut' tai 'läsnä'.")
          false))))

(defn -handleUpdatedOpiskeluoikeus
  "Hakee päivitettyjä opiskeluoikeuksia koskesta ja tallentaa niiden tiedot
  tietokantaan."
  [_ event ^com.amazonaws.services.lambda.runtime.Context context]
  (log-caller-details-scheduled "handleUpdatedOpiskeluoikeus" event context)
  (let [start-time (System/currentTimeMillis)
        last-checked (:value (ddb/get-item
                               {:key [:s "opiskeluoikeus-last-checked"]}
                               (:metadata-table env)))
        last-page (Integer/valueOf
                    ^String (:value (ddb/get-item
                                      {:key [:s "opiskeluoikeus-last-page"]}
                                      (:metadata-table env))))]
    (log/info "Käsitellään" last-checked "jälkeen muuttuneet opiskeluoikeudet")
    (loop [opiskeluoikeudet (k/get-updated-opiskeluoikeudet last-checked
                                                            last-page)
           next-page (inc last-page)]
      (if (seq opiskeluoikeudet)
        (do (doseq [opiskeluoikeus opiskeluoikeudet]
              (let [koulutustoimija (get-koulutustoimija-oid opiskeluoikeus)
                    vahvistus-pvm (get-vahvistus-pvm opiskeluoikeus)]
                (when (and (some? vahvistus-pvm)
                           (check-valid-herate-date vahvistus-pvm)
                           (whitelisted-organisaatio?!
                             koulutustoimija
                             (date-string-to-timestamp
                               vahvistus-pvm))
                           (nil? (:sisältyyOpiskeluoikeuteen opiskeluoikeus))
                           (check-tila opiskeluoikeus vahvistus-pvm))
                  (if-let [hoks
                           (try
                             (ehoks/get-hoks-by-opiskeluoikeus
                               (:oid opiskeluoikeus))
                             (catch ExceptionInfo e
                               (if (= 404 (:status (ex-data e)))
                                 (log/info "Opiskeluoikeudella"
                                           (:oid opiskeluoikeus)
                                           "ei HOKSia. Koulutustoimija:"
                                           koulutustoimija)
                                 (throw e))))]
                    (if (:osaamisen-hankkimisen-tarve hoks)
                      (ac/save-herate
                        (parse-herate
                          hoks
                          (get-kysely-type opiskeluoikeus)
                          vahvistus-pvm)
                        opiskeluoikeus
                        koulutustoimija
                        (:koski herate-sources))
                      (log/info "Ei osaamisen hankkimisen tarvetta hoksissa"
                                (:id hoks)))))))
            (log/info "Käsitelty" (count opiskeluoikeudet)
                      "opiskeluoikeutta, sivu" (dec next-page))
            (update-last-page next-page)
            (when (< 120000 (.getRemainingTimeInMillis context))
              (recur
                (k/get-updated-opiskeluoikeudet last-checked next-page)
                (inc next-page))))
        (do
          (update-last-page 0)
          (update-last-checked (c/from-long start-time)))))))
