(ns oph.heratepalvelu.tep.rahoitusLaskentaHandler
  "Käsittelee työpaikkajaksoja, tallentaa niitä tietokantaan erilliseen tauluun. Rakennettu syksyllä 2022 rahoituskauden
  tietojen päivittämiseen ja uudelleenkäsittelyyn. Voi olla hyödyksi uudelleenkäsittelytarpeisiin muokkauksin, ei tarkoitettu
  jatkuvaan käyttöön."
  (:require [cheshire.core :refer [parse-string]]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.external.ehoks :as ehoks]
            [oph.heratepalvelu.external.koski :as koski]
            [oph.heratepalvelu.log.caller-log :refer [log-caller-details-sqs]]
            [schema.core :as s])
  (:import (clojure.lang ExceptionInfo)
           (com.amazonaws.services.lambda.runtime.events SQSEvent
                                                         SQSEvent$SQSMessage)
           (com.fasterxml.jackson.core JsonParseException)
           (java.time LocalDate DayOfWeek)
           (software.amazon.awssdk.awscore.exception AwsServiceException)
           (software.amazon.awssdk.services.dynamodb.model
             ConditionalCheckFailedException)))

(gen-class
  :name "oph.heratepalvelu.tep.rahoitusLaskentaHandler"
  :methods [[^:static handleRahoitusHerate
             [com.amazonaws.services.lambda.runtime.events.SQSEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(s/defschema tep-herate-keskeytymisajanjakso-schema
  "Keskeytymisajanjakson schema."
  {:alku                   (s/conditional not-empty s/Str)
   (s/optional-key :loppu) (s/maybe s/Str)})

(s/defschema tep-herate-schema
  "TEP-herätteen schema."
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
   :tyopaikkaohjaaja-nimi  (s/conditional not-empty s/Str)
   (s/optional-key :osa-aikaisuus)                  (s/maybe s/Num)
   (s/optional-key :oppisopimuksen-perusta)         (s/maybe s/Str)
   (s/optional-key :tyopaikkaohjaaja-puhelinnumero) (s/maybe s/Str)
   (s/optional-key :keskeytymisajanjaksot)          (s/maybe
                                                      [tep-herate-keskeytymisajanjakso-schema])})

(def tep-herate-checker
  "TEP-herätescheman tarkistusfunktio."
  (s/checker tep-herate-schema))

(defn check-duplicate-hankkimistapa
  "Palauttaa true, jos ei ole vielä jaksoa tietokannassa annetulla ID:llä."
  [id]
  (if (empty? (ddb/get-item {:hankkimistapa_id [:n id]}
                            (:jaksotunnus-table env)))
    true
    (log/warn "Osaamisenhankkimistapa id" id "on jo käsitelty.")))

(defn check-duplicate-tunnus
  "Palauttaa true, jos ei ole vielä jaksoa tietokannassa, jonka tunnus täsmää
  annetun arvon kanssa."
  [tunnus]
  (let [items (ddb/query-items {:tunnus [:eq [:s tunnus]]}
                               {:index "uniikkiusIndex"}
                               (:jaksotunnus-table env))]
    (if (empty? items)
      true
      (throw (ex-info (str "Tunnus " tunnus " on jo käytössä.")
                      {:items items})))))

(defn check-opiskeluoikeus-tila
  "Palauttaa true, jos opiskeluoikeus ei ole terminaalitilassa (eronnut,
  katsotaan eronneeksi, mitätöity, peruutettu, tai väliaikaisesti keskeytynyt)."
  [opiskeluoikeus loppupvm]
  (let [tilat (:opiskeluoikeusjaksot (:tila opiskeluoikeus))
        voimassa (reduce
                   (fn [res next]
                     (if (c/is-before (LocalDate/parse (:alku next))
                                      (LocalDate/parse loppupvm))
                       next
                       (reduced res)))
                   (sort-by :alku tilat))
        tila (:koodiarvo (:tila voimassa))]
    (if (or (= tila "eronnut")
            (= tila "katsotaaneronneeksi")
            (= tila "mitatoity")
            (= tila "peruutettu")
            (= tila "valiaikaisestikeskeytynyt"))
      (log/warn "Opiskeluoikeus" (:oid opiskeluoikeus) "terminaalitilassa" tila)
      true)))

(defn sort-process-keskeytymisajanjaksot
  "Järjestää TEP-jakso keskeytymisajanjaksot, parsii niiden alku- ja
  loppupäivämäärät LocalDateiksi, ja palauttaa tuloslistan."
  [herate]
  (map (fn [x] {:alku (LocalDate/parse (:alku x))
                :loppu (if (:loppu x) (LocalDate/parse (:loppu x)) nil)})
       (sort-by :alku (:keskeytymisajanjaksot herate []))))

(defn check-not-fully-keskeytynyt
  "Palauttaa true, jos TEP-jakso ei ole keskeytynyt sen loppupäivämäärällä."
  [herate]
  (let [kjaksot (sort-process-keskeytymisajanjaksot herate)]
    (or (empty? kjaksot)
        (not (:loppu (last kjaksot)))
        (c/is-after (LocalDate/parse (:loppupvm herate))
                    (:loppu (last kjaksot))))))

(defn check-open-keskeytymisajanjakso
  "Palauttaa true, jos TEP-jakson viimeisellä keskeytymisajanjaksolla ei ole
  loppupäivämäärää."
  [herate]
  (let [kjaksot (sort-process-keskeytymisajanjaksot herate)]
    (and (seq kjaksot)
         (not (:loppu (last kjaksot))))))

(defn save-results-to-table
  [results]
  (ddb/put-item results
                {:cond-expr (str "attribute_not_exists(hankkimistapa_id)")}
                (or (:jaksotunnus-table env)
                    (:jaksotunnus_table env))))

(defn save-results
  "Käsittelee herätteen, varmistaa, että se tulee tallentaa ja tallentaa dynamoon."
  [herate opiskeluoikeus koulutustoimija]
  (let [tapa-id (:hankkimistapa-id herate)]
    (when (check-duplicate-hankkimistapa tapa-id)
      (try
        (let [request-id    (c/generate-uuid)
              niputuspvm    (c/next-niputus-date (str (c/local-date-now)))
              alkupvm       (c/next-niputus-date (:loppupvm herate))
              suoritus      (c/get-suoritus opiskeluoikeus)
              tutkinto      (get-in suoritus [:koulutusmoduuli
                                              :tunniste
                                              :koodiarvo])
              rahoitusryhma (c/get-rahoitusryhma opiskeluoikeus (LocalDate/parse (:loppupvm herate)))
              db-data {:hankkimistapa_id     [:n tapa-id]
                       :hankkimistapa_tyyppi
                                             [:s (last (str/split (:hankkimistapa-tyyppi herate)
                                                                  #"_"))]
                       :tyopaikan_nimi       [:s (:tyopaikan-nimi herate)]
                       :tyopaikan_ytunnus    [:s (:tyopaikan-ytunnus herate)]
                       :ohjaaja_nimi      [:s (:tyopaikkaohjaaja-nimi herate)]
                       :jakso_alkupvm        [:s (:alkupvm herate)]
                       :jakso_loppupvm       [:s (:loppupvm herate)]
                       :request_id           [:s request-id]
                       :tutkinto             [:s tutkinto]
                       :oppilaitos    [:s (:oid (:oppilaitos opiskeluoikeus))]
                       :hoks_id              [:n (:hoks-id herate)]
                       :opiskeluoikeus_oid   [:s (:oid opiskeluoikeus)]
                       :oppija_oid           [:s (:oppija-oid herate)]
                       :koulutustoimija      [:s koulutustoimija]
                       :niputuspvm           [:s (str niputuspvm)]
                       :tpk-niputuspvm       [:s "ei_maaritelty"]
                       :alkupvm              [:s (str alkupvm)]
                       :viimeinen_vastauspvm [:s (str (.plusDays alkupvm 60))]
                       :rahoituskausi        [:s (c/kausi (:loppupvm herate))]
                       :tallennuspvm         [:s (str (c/local-date-now))]
                       :tutkinnonosa_tyyppi  [:s (:tyyppi herate)]
                       :tutkinnonosa_id      [:n (:tutkinnonosa-id herate)]
                       :tutkintonimike
                                             [:s (str (seq (map :koodiarvo
                                                                (:tutkintonimike suoritus))))]
                       :osaamisala
                                             [:s (str (seq (arvo/get-osaamisalat
                                                             suoritus
                                                             (:oid opiskeluoikeus))))]
                       :toimipiste_oid [:s (str (arvo/get-toimipiste suoritus))]
                       :ohjaaja_ytunnus_kj_tutkinto
                                             [:s (str (:tyopaikkaohjaaja-nimi herate) "/"
                                                      (:tyopaikan-ytunnus herate) "/"
                                                      koulutustoimija "/" tutkinto)]
                       :tyopaikan_normalisoitu_nimi
                                             [:s (c/normalize-string (:tyopaikan-nimi herate))]
                       :rahoitusryhma        [:s rahoitusryhma]}
              jaksotunnus-table-data
              (cond-> db-data
                      (not-empty (:tyopaikkaohjaaja-email herate))
                      (assoc :ohjaaja_email [:s (:tyopaikkaohjaaja-email herate)])
                      (not-empty (:tyopaikkaohjaaja-puhelinnumero herate))
                      (assoc :ohjaaja_puhelinnumero
                             [:s (:tyopaikkaohjaaja-puhelinnumero herate)])
                      (not-empty (:tutkinnonosa-koodi herate))
                      (assoc :tutkinnonosa_koodi [:s (:tutkinnonosa-koodi herate)])
                      (not-empty (:tutkinnonosa-nimi herate))
                      (assoc :tutkinnonosa_nimi [:s (:tutkinnonosa-nimi herate)])
                      (some? (:osa-aikaisuus herate))
                      (assoc :osa_aikaisuus [:n (:osa-aikaisuus herate)])
                      (some? (:oppisopimuksen-perusta herate))
                      (assoc :oppisopimuksen_perusta
                             [:s (last
                                   (str/split
                                     (:oppisopimuksen-perusta herate)
                                     #"_"))]))
             ]
          (if (check-open-keskeytymisajanjakso herate)
            (try
              (save-results-to-table jaksotunnus-table-data) ;näille ei normaalikäsittelyssä luotu arvo-tunnusta.
              (catch ConditionalCheckFailedException e
                (log/warn "Osaamisenhankkimistapa id:llä"
                          tapa-id
                          "on jo käsitelty."))
              (catch AwsServiceException e
                (log/error "Virhe tietokantaan tallennettaessa, request-id"
                           request-id)
                (throw e)))
            (let [existing-arvo-tunnus "unknown"] ;fixme todo yritetään päätellä dynamosta mahdollisesti jo löytyvä arvo-tunnus
              (log/info "Heräte with non-open-keskeytymisajanjakso - fixme, needs implementation " tapa-id)
              )))
        (catch Exception e
          (log/error "Unknown error" e)
          (throw e))))))

(defn -handleRahoitusHerate
  "Käsittelee jaksoherätteet, jotka eHOKS-palvelu lähettää SQS:in kautta. Tekee
  joitakin tarkistaksia ja tallentaa herätteen tietokantaan, jos testit
  läpäistään."
  [_ ^SQSEvent event context]
  (log-caller-details-sqs "-handleRahoitusHerate" context)
  (let [messages (seq (.getRecords event))]
    (doseq [^SQSEvent$SQSMessage msg messages]
      (try
        (let [herate (parse-string (.getBody msg) true)
              opiskeluoikeus (koski/get-opiskeluoikeus-catch-404
                               (:opiskeluoikeus-oid herate))]
          (when (some? opiskeluoikeus)
            (let [koulutustoimija (c/get-koulutustoimija-oid opiskeluoikeus)]
              (if (some? (tep-herate-checker herate))
                (log/error {:herate herate :msg (tep-herate-checker herate)})
                (when (and (check-opiskeluoikeus-tila opiskeluoikeus
                                                      (:loppupvm herate))
                           (check-not-fully-keskeytynyt herate)
                           (c/check-opiskeluoikeus-suoritus-types?
                             opiskeluoikeus)
                           (c/check-sisaltyy-opiskeluoikeuteen? opiskeluoikeus))
                  (save-results herate opiskeluoikeus koulutustoimija)))))
          (do
            (log/info "No opiskeluoikeus found for oid " (:opiskeluoikeus-oid herate))
            (log/info "Not setting tep-kasitelty - FIXME - hankkimistapa-id " (:hankkimistapa-id herate))
            ;(ehoks/patch-oht-tep-kasitelty (:hankkimistapa-id herate)))
            ))
        (catch JsonParseException e
          (log/error "Virhe viestin lukemisessa:" e))
        (catch ExceptionInfo e
          (if (and (:status (ex-data e))
                   (= 404 (:status (ex-data e))))
            (do
              (log/error "Ei opiskeluoikeutta"
                         (:opiskeluoikeus-oid (parse-string (.getBody msg)
                                                            true)))
              (log/error "Virhe:" e))
            (do (log/error e)
                (throw e))))))))
