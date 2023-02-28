(ns oph.heratepalvelu.tep.jaksoHandler
  "Käsittelee työpaikkajaksoja, tallentaa niitä tietokantaan, ja valmistaa niitä
  niputukseen."
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
           (java.time LocalDate)
           (software.amazon.awssdk.awscore.exception AwsServiceException)
           (software.amazon.awssdk.services.dynamodb.model
             ConditionalCheckFailedException)))

(gen-class
  :name "oph.heratepalvelu.tep.jaksoHandler"
  :methods [[^:static handleJaksoHerate
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
   (s/optional-key :keskeytymisajanjaksot)
   (s/maybe [tep-herate-keskeytymisajanjakso-schema])})

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

(defn sort-process-keskeytymisajanjaksot
  "Järjestää TEP-jakso keskeytymisajanjaksot, parsii niiden alku- ja
  loppupäivämäärät LocalDateiksi, ja palauttaa tuloslistan."
  [herate]
  (map c/alku-and-loppu-to-localdate
       (sort-by :alku (:keskeytymisajanjaksot herate))))

(defn fully-keskeytynyt?
  "Palauttaa true, jos TEP-jakso on keskeytynyt sen loppupäivämäärällä."
  [herate]
  (let [kjaksot (sort-process-keskeytymisajanjaksot herate)]
    (when-let [kjakso-loppu (:loppu (last kjaksot))]
      (not (c/is-after (LocalDate/parse (:loppupvm herate)) kjakso-loppu)))))

(defn loppupvm-in-last-keskeytymisajanjakso?
  "Palauttaa true, jos TEP-jakson viimeisessä keskeytymisajanjaksossa on
  loppupäivämäärää."
  [herate]
  (some? (:loppu (last (sort-by :alku (:keskeytymisajanjaksot herate))))))

(defn save-to-tables
  "Tallentaa jakso ja nipun tietokantaan."
  [jaksotunnus-table-data nippu-table-data]
  (ddb/put-item jaksotunnus-table-data
                {:cond-expr (str "attribute_not_exists(hankkimistapa_id)")}
                (:jaksotunnus-table env))
  (let [oykt (second (:ohjaaja_ytunnus_kj_tutkinto nippu-table-data))
        niputuspvm (second (:niputuspvm nippu-table-data))
        existing-nippu (ddb/get-item
                         {:ohjaaja_ytunnus_kj_tutkinto [:s oykt]
                          :niputuspvm                  [:s niputuspvm]}
                         (:nippu-table env))]
    (when (or (empty? existing-nippu)
              (and (= (:kasittelytila existing-nippu)
                      (:ei-niputeta c/kasittelytilat))
                   (= (:sms_kasittelytila existing-nippu)
                      (:ei-niputeta c/kasittelytilat))))
      (ddb/put-item nippu-table-data {} (:nippu-table env)))))

(defn save-jaksotunnus
  "Käsittelee herätteen, varmistaa, että se tulee tallentaa, hakee
  jaksotunnuksen Arvosta, luo jakson ja alusatavan nipun, ja tallentaa ne
  tietokantaan."
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
              rahoitusryhma (c/get-rahoitusryhma
                              opiskeluoikeus
                              (LocalDate/parse (:loppupvm herate)))
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
              nippu-table-data
              {:ohjaaja_ytunnus_kj_tutkinto
               [:s (str (:tyopaikkaohjaaja-nimi herate) "/"
                        (:tyopaikan-ytunnus herate) "/"
                        koulutustoimija "/" tutkinto)]
               :ohjaaja               [:s (:tyopaikkaohjaaja-nimi herate)]
               :ytunnus               [:s (:tyopaikan-ytunnus herate)]
               :tyopaikka             [:s (:tyopaikan-nimi herate)]
               :koulutuksenjarjestaja [:s koulutustoimija]
               :tutkinto              [:s tutkinto]
               :kasittelytila         [:s (:ei-niputettu c/kasittelytilat)]
               :sms_kasittelytila     [:s (:ei-lahetetty c/kasittelytilat)]
               :niputuspvm            [:s (str niputuspvm)]}]
          (if (and (:keskeytymisajanjaksot herate)
                   (not (loppupvm-in-last-keskeytymisajanjakso? herate)))
            (try
              (save-to-tables
                jaksotunnus-table-data
                (assoc nippu-table-data
                       :kasittelytila     [:s (:ei-niputeta c/kasittelytilat)]
                       :sms_kasittelytila [:s (:ei-niputeta c/kasittelytilat)]))
              (catch ConditionalCheckFailedException _
                (log/warn "Osaamisenhankkimistapa id:llä"
                          tapa-id
                          "on jo käsitelty."))
              (catch AwsServiceException e
                (log/error "Virhe tietokantaan tallennettaessa, request-id"
                           request-id)
                (throw e)))
            (let [arvo-resp (arvo/create-jaksotunnus
                              (arvo/build-jaksotunnus-request-body
                                herate
                                (c/normalize-string (:tyopaikan-nimi herate))
                                opiskeluoikeus
                                request-id
                                koulutustoimija
                                suoritus
                                (str alkupvm)))
                  tunnus (:tunnus (:body arvo-resp))]
              (try
                (when (and (some? tunnus) (check-duplicate-tunnus tunnus))
                  (save-to-tables
                    (assoc jaksotunnus-table-data :tunnus [:s tunnus])
                    nippu-table-data))
                (catch ConditionalCheckFailedException _
                  (log/warn "Osaamisenhankkimistapa id:llä"
                            tapa-id
                            "on jo käsitelty.")
                  (arvo/delete-jaksotunnus tunnus))
                (catch AwsServiceException e
                  (log/error "Virhe tietokantaan tallennettaessa"
                             tunnus
                             request-id)
                  (arvo/delete-jaksotunnus tunnus)
                  (throw e))))))
        (catch Exception e
          (log/error "Unknown error" e)
          (throw e))))))

(defn -handleJaksoHerate
  "Käsittelee jaksoherätteet, jotka eHOKS-palvelu lähettää SQS:n kautta.
  Suorittaa tietojen tarkastuksia ja tallentaa herätteen tietokantaan, jos
  testit läpäistään."
  [_ ^SQSEvent event context]
  (log-caller-details-sqs "handleTPOherate" context)
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
                (when-not
                 (or (c/terminaalitilassa? opiskeluoikeus (:loppupvm herate))
                     (fully-keskeytynyt? herate)
                     (not (c/has-one-or-more-ammatillinen-tutkinto?
                            opiskeluoikeus))
                     (c/feedback-collecting-prevented? opiskeluoikeus
                                                       (:loppupvm herate))
                     (c/sisaltyy-toiseen-opiskeluoikeuteen? opiskeluoikeus))
                  (save-jaksotunnus herate opiskeluoikeus koulutustoimija)))))
          (ehoks/patch-oht-tep-kasitelty (:hankkimistapa-id herate)))
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
