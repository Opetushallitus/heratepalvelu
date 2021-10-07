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
            [oph.heratepalvelu.external.ehoks :as ehoks]
            [clojure.string :as str])
  (:import (com.fasterxml.jackson.core JsonParseException)
           (clojure.lang ExceptionInfo)
           (software.amazon.awssdk.services.dynamodb.model ConditionalCheckFailedException)
           (software.amazon.awssdk.awscore.exception AwsServiceException)
           (java.time LocalDate DayOfWeek)))

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
              :tyopaikkaohjaaja-nimi  (s/conditional not-empty s/Str)
              (s/optional-key :osa-aikaisuus)                  (s/maybe s/Num)
              (s/optional-key :oppisopimuksen-perusta)         (s/maybe s/Str)
              (s/optional-key :tyopaikkaohjaaja-puhelinnumero) (s/maybe s/Str)})

(def tep-herate-checker
  (s/checker tep-herate-schema))

(defn check-duplicate-hankkimistapa [id]
  (if (empty? (ddb/get-item {:hankkimistapa_id [:n id]}
                            (:jaksotunnus-table env)))
    true
    (log/warn "Osaamisenhankkimistapa id:llä " id "on jo käsitelty.")))

(defn check-duplicate-tunnus [tunnus]
  (let [items (ddb/query-items {:tunnus [:eq [:s tunnus]]}
                               {:index "uniikkiusIndex"}
                               (:jaksotunnus-table env))]
    (if (empty? items)
      true
      (throw (ex-info (str "Tunnus " tunnus " on jo käytössä. ") {:items items})))))

(defn check-opiskeluoikeus-tila [opiskeluoikeus loppupvm]
  (let [tilat (:opiskeluoikeusjaksot (:tila opiskeluoikeus))
        voimassa (reduce
                   (fn [res next]
                     (if (.isBefore (LocalDate/parse (:alku next)) (LocalDate/parse loppupvm))
                       next
                       (reduced res)))
                   (sort-by :alku tilat))
        tila (:koodiarvo (:tila voimassa))]
    (if (or (= tila "eronnut")
            (= tila "katsotaaneronneeksi")
            (= tila "mitatoity")
            (= tila "peruutettu")
            (= tila "valiaikaisestikeskeytynyt"))
      (log/warn "Opiskeluoikeus " (:oid opiskeluoikeus) " terminaalitilassa " tila)
      true)))

(defn kesto [herate oo-tilat]
  (let [alku-date (LocalDate/parse (:alkupvm herate))
        loppu-date (LocalDate/parse (:loppupvm herate))
        sorted-tilat (map #(assoc %1
                             :alku
                             (LocalDate/parse (:alku %1))
                             :tila
                             (:koodiarvo (:tila %1)))
                          (sort-by :alku oo-tilat))
        voimassa (reduce
                   (fn [res next]
                     (if (and (.isBefore (:alku (first res)) alku-date)
                              (not (.isAfter (:alku next) alku-date)))
                       (rest res)
                       (reduced res)))
                   sorted-tilat
                   (rest sorted-tilat))]
    (loop [kesto 1
           pvm alku-date
           tilat voimassa]
      (if (.isBefore pvm loppu-date)
        (let
          [new-kesto (if (or (= (.getDayOfWeek pvm) DayOfWeek/SATURDAY)
                             (= (.getDayOfWeek pvm) DayOfWeek/SUNDAY)
                             (= "valiaikaisestikeskeytynyt" (:tila (first tilat)))
                             (= "loma" (:tila (first tilat))))
                       kesto
                       (+ 1 kesto))
           new-pvm (.plusDays pvm 1)]
          (recur new-kesto
                 new-pvm
                 (if (and (some? (second tilat))
                          (not (.isAfter (:alku (second tilat)) new-pvm)))
                   (rest tilat)
                   tilat)))
        (if (and (some? (:osa-aikaisuus herate))
                 (< 0   (:osa-aikaisuus herate))
                 (> 100 (:osa-aikaisuus herate)))
          (int (Math/ceil (/ (* kesto (:osa-aikaisuus herate)) 100)))
          kesto)))))

(defn save-jaksotunnus [herate opiskeluoikeus koulutustoimija]
  (let [tapa-id (:hankkimistapa-id herate)]
    (when (check-duplicate-hankkimistapa tapa-id)
      (try
        (let [request-id (c/generate-uuid)
              niputuspvm (c/next-niputus-date (str (LocalDate/now)))
              alkupvm    (c/next-niputus-date (:loppupvm herate))
              suoritus   (c/get-suoritus opiskeluoikeus)
              kesto      (kesto herate (:opiskeluoikeusjaksot (:tila opiskeluoikeus)))
              tutkinto   (get-in
                           suoritus
                           [:koulutusmoduuli
                            :tunniste
                            :koodiarvo])
              arvo-resp  (arvo/create-jaksotunnus
                           (arvo/build-jaksotunnus-request-body
                             herate
                             kesto
                             opiskeluoikeus
                             request-id
                             koulutustoimija
                             suoritus
                             (str alkupvm)))
              tunnus (:tunnus (:body arvo-resp))
              db-data {:hankkimistapa_id     [:n tapa-id]
                       :hankkimistapa_tyyppi [:s (last
                                                   (str/split
                                                     (:hankkimistapa-tyyppi herate)
                                                     #"_"))]
                       :tyopaikan_nimi       [:s (:tyopaikan-nimi herate)]
                       :tyopaikan_ytunnus    [:s (:tyopaikan-ytunnus herate)]
                       :tunnus               [:s tunnus]
                       :ohjaaja_nimi         [:s (:tyopaikkaohjaaja-nimi herate)]
                       :jakso_alkupvm        [:s (:alkupvm herate)]
                       :jakso_loppupvm       [:s (:loppupvm herate)]
                       :kesto                [:n kesto]
                       :request_id           [:s request-id]
                       :tutkinto             [:s tutkinto]
                       :oppilaitos           [:s (:oid (:oppilaitos opiskeluoikeus))]
                       :hoks_id              [:n (:hoks-id herate)]
                       :opiskeluoikeus_oid   [:s (:oid opiskeluoikeus)]
                       :oppija_oid           [:s (:oppija-oid herate)]
                       :koulutustoimija      [:s koulutustoimija]
                       :niputuspvm           [:s (str niputuspvm)]
                       :alkupvm              [:s (str alkupvm)]
                       :viimeinen_vastauspvm [:s (str (.plusDays alkupvm 60))]
                       :rahoituskausi        [:s (c/kausi (:loppupvm herate))]
                       :tallennuspvm         [:s (str (LocalDate/now))]
                       :tutkinnonosa_tyyppi  [:s (:tyyppi herate)]
                       :tutkinnonosa_id      [:n (:tutkinnonosa-id herate)]
                       :ohjaaja_ytunnus_kj_tutkinto
                                             [:s (str
                                                   (:tyopaikkaohjaaja-nimi herate) "/"
                                                   (:tyopaikan-ytunnus herate) "/"
                                                   koulutustoimija "/"
                                                   tutkinto)]}]
          (when (and (some? tunnus)
                     (check-duplicate-tunnus tunnus))
            (try
              (ddb/put-item
                (cond-> db-data
                        (not-empty (:tyopaikkaohjaaja-email herate))
                        (assoc :ohjaaja_email [:s (:tyopaikkaohjaaja-email herate)])
                        (not-empty (:tyopaikkaohjaaja-puhelinnumero herate))
                        (assoc :ohjaaja_puhelinnumero [:s (:tyopaikkaohjaaja-puhelinnumero herate)])
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
                {:cond-expr (str "attribute_not_exists(hankkimistapa_id)")}
                (:jaksotunnus-table env))
              (ddb/put-item
                {:ohjaaja_ytunnus_kj_tutkinto [:s (str
                                                    (:tyopaikkaohjaaja-nimi herate) "/"
                                                    (:tyopaikan-ytunnus herate) "/"
                                                    koulutustoimija "/"
                                                    tutkinto)]
                 :ohjaaja                     [:s (:tyopaikkaohjaaja-nimi herate)]
                 :ytunnus                     [:s (:tyopaikan-ytunnus herate)]
                 :tyopaikka                   [:s (:tyopaikan-nimi herate)]
                 :koulutuksenjarjestaja       [:s koulutustoimija]
                 :tutkinto                    [:s tutkinto]
                 :kasittelytila               [:s (:ei-niputettu c/kasittelytilat)]
                 :sms_kasittelytila           [:s (:ei-lahetetty c/kasittelytilat)]
                 :niputuspvm                  [:s (str niputuspvm)]}
                {} (:nippu-table env))
              (catch ConditionalCheckFailedException e
                (log/warn "Osaamisenhankkimistapa id:llä " tapa-id "on jo käsitelty.")
                (arvo/delete-jaksotunnus tunnus))
              (catch AwsServiceException e
                (log/error "Virhe tietokantaan tallennettaessa " tunnus " " request-id)
                (arvo/delete-jaksotunnus tunnus)
                (throw e)))))
        (catch Exception e
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
                (check-opiskeluoikeus-tila opiskeluoikeus (:loppupvm herate))
                (c/check-organisaatio-whitelist? koulutustoimija)
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
                (= 404 (:status (ex-data e))))
            (log/error "Ei opiskeluoikeutta " (:opiskeluoikeus-oid
                                                (parse-string (.getBody msg) true)))
            (do (log/error e)
                (throw e))))))))
