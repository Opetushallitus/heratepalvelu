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
    (some #(and (ammatillisen-tutkinnon-suoritus? %)
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

(defn handle-single-opiskeluoikeus!
  "Käsittelee yhden päivittyneen opiskeluoikeudet (vie sen herätteenä kantaan)."
  [opiskeluoikeus]
  (log/info "käsitellään päivittynyt opiskeluoikeus" (:oid opiskeluoikeus))
  (let [koulutustoimija (get-koulutustoimija-oid opiskeluoikeus)
        vahvistus-pvm (get-vahvistus-pvm opiskeluoikeus)]
    (cond
      (or (not vahvistus-pvm) (not (valid-herate-date? vahvistus-pvm)))
      (log/warn "huono vahvistuspäivämäärä: " vahvistus-pvm)

      (not (whitelisted-organisaatio?!
             koulutustoimija (date-string-to-timestamp vahvistus-pvm)))
      (log/info "koulutustoimijaa" koulutustoimija "ei käsitellä")

      (:sisältyyOpiskeluoikeuteen opiskeluoikeus)
      (log/info "opiskeluoikeus sisältyy toiseen"
                (:sisältyyOpiskeluoikeuteen opiskeluoikeus) ", ei käsitellä")

      (not (check-tila opiskeluoikeus vahvistus-pvm))
      (log/info "opiskeluoikeus on tilassa"
                (get-tila opiskeluoikeus vahvistus-pvm) "joten ei käsitellä")

      :else
      (try
        (let [hoks (ehoks/get-hoks-by-opiskeluoikeus (:oid opiskeluoikeus))
              herate (parse-herate
                       hoks (get-kysely-type opiskeluoikeus) vahvistus-pvm)]
          (if-not (:osaamisen-hankkimisen-tarve hoks)
            (log/info "Ei osaamisen hankkimisen tarvetta hoksissa" (:id hoks))
            (ac/check-and-save-herate! herate opiskeluoikeus koulutustoimija
                                       (:koski herate-sources))))
        (catch ExceptionInfo e
          (if (= 404 (:status (ex-data e)))
            (log/info "Opiskeluoikeudella" (:oid opiskeluoikeus)
                      "ei HOKSia. Koulutustoimija:" koulutustoimija)
            (log/error e "at handle-single-opiskeluoikeus!")))
        (catch Exception e
          (log/error e "at handle-single-opiskeluoikeus!"))))))

(defn fetch-and-process-opiskeluoikeudet-from!
  "Hakee opiskeluoikeuksien päivitykset annetusta (ajan-)kohdasta eteenpäin
  ja vie niiden tiedot tietokantaan."
  [last-checked last-page
   ^com.amazonaws.services.lambda.runtime.Context context]
  (if-some [opiskeluoikeudet
            (seq (k/get-updated-opiskeluoikeudet last-checked last-page))]
    (do
      (log/info "Käsitellään" (count opiskeluoikeudet)
                "opiskeluoikeutta, sivu" last-page)
      (doseq [opiskeluoikeus opiskeluoikeudet]
        (handle-single-opiskeluoikeus! opiskeluoikeus))
      (update-last-page (inc last-page))
      (when (< 120000 (.getRemainingTimeInMillis context))
        (recur last-checked (inc last-page) context)))
    (log/info "Ei enempiä päivittyneitä opiskeluoikeuksia")))

(defn -handleUpdatedOpiskeluoikeus
  "Hakee päivitettyjä opiskeluoikeuksia koskesta ja tallentaa niiden tiedot
  tietokantaan. Jos kaikki menee hyvin, päivittää mistä eteenpäin seuraavaksi
  tiedot luetaan."
  [_ event ^com.amazonaws.services.lambda.runtime.Context context]
  (log-caller-details-scheduled "handleUpdatedOpiskeluoikeus" event context)
  (let [start-time (current-time-millis)
        last-checked (:value (ddb/get-item
                               {:key [:s "opiskeluoikeus-last-checked"]}
                               (:metadata-table env)))
        last-page (Integer/valueOf
                    ^String (:value (ddb/get-item
                                      {:key [:s "opiskeluoikeus-last-page"]}
                                      (:metadata-table env))))]
    (log/info "Käsitellään" last-checked "jälkeen muuttuneet opiskeluoikeudet")
    (fetch-and-process-opiskeluoikeudet-from! last-checked last-page context)
    (update-last-page 0)
    (update-last-checked (c/from-long start-time))))
