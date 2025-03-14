(ns oph.heratepalvelu.tep.rahoitusLaskentaHandler
  "Käsittelee työpaikkajaksoja, tallentaa niitä tietokantaan erilliseen tauluun.
  Rakennettu syksyllä 2022 rahoituskauden tietojen päivittämiseen ja
  uudelleenkäsittelyyn. Voi olla hyödyksi uudelleenkäsittelytarpeisiin
  muokkauksin, ei tarkoitettu jatkuvaan käyttöön.

  HUOM! Tätä handleria ei ole käytetty eikä ylläpidetty hyvään toviin, joten
  jos suunnittelet handlerin käyttöä, varmista että se pitää sisällään
  17. lokakuuta 2022 jälkeen tehdyt muutokset jaksoHandleriin sekä
  kestonlaskentaan."
  (:require [cheshire.core :refer [parse-string]]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.external.koski :as koski]
            [oph.heratepalvelu.log.caller-log :refer [log-caller-details-sqs]]
            [oph.heratepalvelu.tep.jaksoHandler :as jh]
            [oph.heratepalvelu.tep.niputusHandler :as nh]
            [oph.heratepalvelu.util.string :as u-str])
  (:import (clojure.lang ExceptionInfo)
           (com.amazonaws.services.lambda.runtime.events SQSEvent
                                                         SQSEvent$SQSMessage)
           (software.amazon.awssdk.awscore.exception AwsServiceException)
           (software.amazon.awssdk.services.dynamodb.model
             ConditionalCheckFailedException)))

(gen-class
  :name "oph.heratepalvelu.tep.rahoitusLaskentaHandler"
  :methods [[^:static handleRahoitusHerate
             [com.amazonaws.services.lambda.runtime.events.SQSEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn read-previously-processed-hankkimistapa
  "Palauttaa true, jos ei ole vielä jaksoa tietokannassa annetulla ID:llä."
  [id]
  (ddb/get-item {:hankkimistapa_id [:n id]}
                (:jaksotunnus-table env)))

(defn save-results-to-ddb
  [results]
  (let [table (:results-table env)]
    (log/info (str "Saving results to table " table ": " results)))
  (ddb/put-item results
                {}
                (:results-table env)))

(defn math-round "Wrapper Math/round:in ympäri" [^double x] (Math/round x))

(defn save-results
  "Käsittelee herätteen ja tallentaa dynamoon."
  [herate opiskeluoikeus koulutustoimija]
  (let [tapa-id (:hankkimistapa-id herate)
        start-time (System/currentTimeMillis)]
    (log/info "saving results for tapa-id " tapa-id)
    (try
      (let [request-id    (c/generate-uuid)
            niputuspvm    (c/next-niputus-date (str (c/local-date-now)))
            alkupvm       (c/next-niputus-date (:loppupvm herate))
            suoritus      (c/get-suoritus opiskeluoikeus)
            tutkinto      (get-in suoritus [:koulutusmoduuli
                                            :tunniste
                                            :koodiarvo])
            existing-arvo-tunnus
            (:tunnus (read-previously-processed-hankkimistapa tapa-id))
            jakso {:hankkimistapa_id tapa-id
                   :oppija_oid (:oppija-oid herate)
                   :jakso_alkupvm (:alkupvm herate)
                   :jakso_loppupvm (:loppupvm herate)}
            [kesto kesto-vanha] (map #(get % (:hankkimistapa_id jakso))
                                     (nh/jaksojen-kestot! [jakso]))
            db-data {:hankkimistapa_id     [:n tapa-id]
                     :hankkimistapa_tyyppi
                     [:s (last (str/split (:hankkimistapa-tyyppi herate) #"_"))]
                     :tyopaikan_nimi       [:s (:tyopaikan-nimi herate)]
                     :tyopaikan_ytunnus    [:s (:tyopaikan-ytunnus herate)]
                     :ohjaaja_nimi         [:s (:tyopaikkaohjaaja-nimi herate)]
                     :jakso_alkupvm        [:s (:alkupvm herate)]
                     :jakso_loppupvm       [:s (:loppupvm herate)]
                     :request_id           [:s request-id]
                     :tutkinto             [:s tutkinto]
                     :oppilaitos      [:s (:oid (:oppilaitos opiskeluoikeus))]
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
                     :toimipiste_oid
                     [:s (str (arvo/get-toimipiste suoritus))]
                     :ohjaaja_ytunnus_kj_tutkinto
                     [:s (str (:tyopaikkaohjaaja-nimi herate) "/"
                              (:tyopaikan-ytunnus herate) "/"
                              koulutustoimija "/" tutkinto)]
                     :tyopaikan_normalisoitu_nimi
                     [:s (u-str/normalize (:tyopaikan-nimi herate))]
                     :existing-arvo-tunnus [:s (str existing-arvo-tunnus)]
                     :vanha-kesto           [:n kesto-vanha]
                     ; NOTE: Uudessa laskutavassa osa-aikaisuutta ei oteta
                     ;       huomioon
                     ; :uusi-kesto-with-oa    [:n kesto]
                     ; :uusi-kesto-without-oa [:n kesto]
                     :uusi-kesto [:n kesto]
                     :save-timestamp [:s (str start-time)]}
            results-table-data
            (cond-> db-data
              (not-empty (:tyopaikkaohjaaja-email herate))
              (assoc :ohjaaja_email [:s (:tyopaikkaohjaaja-email herate)])
              (not-empty (:tyopaikkaohjaaja-puhelinnumero herate))
              (assoc :ohjaaja_puhelinnumero
                     [:s (:tyopaikkaohjaaja-puhelinnumero herate)])
              (not-empty (:tutkinnonosa-koodi herate))
              (assoc :tutkinnonosa_koodi
                     [:s (:tutkinnonosa-koodi herate)])
              (not-empty (:tutkinnonosa-nimi herate))
              (assoc :tutkinnonosa_nimi [:s (:tutkinnonosa-nimi herate)])
              (some? (:osa-aikaisuus herate))
              (assoc :osa_aikaisuus [:n (:osa-aikaisuus herate)])
              (some? (:oppisopimuksen-perusta herate))
              (assoc :oppisopimuksen_perusta
                     [:s (last
                           (str/split
                             (:oppisopimuksen-perusta herate)
                             #"_"))]))]
        (log/info (str "Uudelleenlaskettu kesto tapa-id:lle "
                       tapa-id
                       ": "
                       kesto))
        (when (jh/has-open-keskeytymisajanjakso? herate)
          (log/warn "Herätteellä on avoin keskeytymisajanjakso: " herate))
        (try
          ;; näille ei normaalikäsittelyssä luotu arvo-tunnusta.
          (save-results-to-ddb results-table-data)
          (catch ConditionalCheckFailedException _
            (log/warn "Osaamisenhankkimistapa id:llä"
                      tapa-id
                      "on jo käsitelty."))
          (catch AwsServiceException e
            (log/error "Virhe tietokantaan tallennettaessa, request-id"
                       request-id)
            (throw e))))
      (catch Exception e
        (log/error "Unknown error" e)
        (throw e)))))

(defn -handleRahoitusHerate
  "Käsittelee jaksoherätteet, jotka eHOKS-palvelu lähettää SQS:in kautta. Tekee
  joitakin tarkistaksia ja tallentaa herätteen tietokantaan, jos testit
  läpäistään."
  [_ ^SQSEvent event context]
  (log-caller-details-sqs "-handleRahoitusHerate" context)
  (log/info "handling rahoitusheräte" event)
  (let [messages (seq (.getRecords event))]
    (doseq [^SQSEvent$SQSMessage msg messages]
      (log/info "heräte:" (.getBody msg))
      (try
        (let [herate (parse-string (.getBody msg) true)
              opiskeluoikeus (koski/get-opiskeluoikeus-catch-404!
                               (:opiskeluoikeus-oid herate))]
          (if (nil? opiskeluoikeus)
            (log/warn "No opiskeluoikeus found for oid"
                      (:opiskeluoikeus-oid herate)
                      "hankkimistapa-id" (:hankkimistapa-id herate))
            (let [koulutustoimija (c/get-koulutustoimija-oid opiskeluoikeus)
                  schema-errors (jh/tep-herate-checker herate)]
              (cond
                (some? schema-errors)
                (log/error "Skeemavirhe:" schema-errors)

                (c/terminaalitilassa? opiskeluoikeus (:loppupvm herate))
                (log/warn "opiskeluoikeus loppunut:" opiskeluoikeus)

                (jh/fully-keskeytynyt? herate)
                (log/warn "jakso keskeytynyt:" herate)

                (not (c/has-one-or-more-ammatillinen-tutkinto? opiskeluoikeus))
                (log/warn "Tutkinto ei ole ammatillinen:" opiskeluoikeus)

                (c/sisaltyy-toiseen-opiskeluoikeuteen? opiskeluoikeus)
                (log/warn "Opiskeluoikeus sisältyy toiseen:" opiskeluoikeus)

                :else
                (jh/save-jaksotunnus herate opiskeluoikeus koulutustoimija)))))
        (catch ExceptionInfo e
          (if (and (:status (ex-data e))
                   (= 404 (:status (ex-data e))))
            (do
              (log/error "Ei opiskeluoikeutta"
                         (:opiskeluoikeus-oid
                          (parse-string (.getBody msg) true)))
              (log/error "Virhe:" e))
            (do (log/error e)
                (throw e))))
        (catch Exception e
          (log/error e "herätteessä" msg))))))
