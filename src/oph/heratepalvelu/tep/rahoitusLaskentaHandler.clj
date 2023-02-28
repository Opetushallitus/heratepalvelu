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
            [oph.heratepalvelu.external.ehoks :as ehoks]
            [oph.heratepalvelu.external.koski :as koski]
            [oph.heratepalvelu.tep.jaksoHandler :as jh]
            [oph.heratepalvelu.tep.niputusHandler :as nh]
            [oph.heratepalvelu.log.caller-log :refer [log-caller-details-sqs]])
  (:import (clojure.lang ExceptionInfo)
           (com.amazonaws.services.lambda.runtime.events SQSEvent
                                                         SQSEvent$SQSMessage)
           (com.fasterxml.jackson.core JsonParseException)
           (java.time LocalDate)
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
            rahoitusryhma (->> (:loppupvm herate)
                               (LocalDate/parse)
                               (c/get-rahoitusryhma opiskeluoikeus))
            jakso {:hankkimistapa_id tapa-id
                   :oppija_oid (:oppija-oid herate)
                   :jakso_alkupvm (:alkupvm herate)
                   :jakso_loppupvm (:loppupvm herate)}
            concurrent-jaksot (ehoks/get-tyoelamajaksot-active-between
                                (:oppija_oid jakso)
                                (:jakso_alkupvm jakso)
                                (:jakso_loppupvm jakso))
            opiskeluoikeudet (nh/get-jaksojen-opiskeluoikeudet
                               (assoc {} (:opiskeluoikeus-oid herate)
                                      opiskeluoikeus)
                               (map :opiskeluoikeus_oid concurrent-jaksot))
            kestot (nh/compute-kesto-old-and-new
                     jakso concurrent-jaksot opiskeluoikeudet)
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
                     [:s (c/normalize-string (:tyopaikan-nimi herate))]
                     :rahoitusryhma        [:s rahoitusryhma]
                     :existing-arvo-tunnus [:s (str existing-arvo-tunnus)]
                     :vanha-kesto
                     [:n (math-round (or (get kestot :vanha) 0.0))]
                     :uusi-kesto-with-oa
                     [:n (math-round (or (get-in kestot [:uusi :with-oa]) 0.0))]
                     :uusi-kesto-without-oa
                     [:n (math-round (or (get-in kestot [:uusi :without-oa])
                                         0.0))]
                     :save-timestamp [:s (str start-time)]}
            results-table-data
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
                             #"_"))]))]
        (log/info "Uudelleenlaskettu kesto tapa-id:lle" tapa-id ":" kestot)
        (when (jh/loppupvm-in-last-keskeytymisajanjakso? herate)
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
  (log/info "handling rahoitusheräte " event)
  (log-caller-details-sqs "-handleRahoitusHerate" context)
  (let [messages (seq (.getRecords event))]
    (doseq [^SQSEvent$SQSMessage msg messages]
      (try
        (let [herate (parse-string (.getBody msg) true)
              opiskeluoikeus (koski/get-opiskeluoikeus-catch-404
                               (:opiskeluoikeus-oid herate))]
          (if (some? opiskeluoikeus)
            (let [koulutustoimija (c/get-koulutustoimija-oid opiskeluoikeus)]
              (if (some? (jh/tep-herate-checker herate))
                (log/error {:herate herate :msg (jh/tep-herate-checker herate)})
                (when-not
                 (or (c/terminaalitilassa? opiskeluoikeus (:loppupvm herate))
                     (jh/fully-keskeytynyt? herate)
                     (not (c/has-one-or-more-ammatillinen-tutkinto?
                            opiskeluoikeus))
                     (c/sisaltyy-toiseen-opiskeluoikeuteen? opiskeluoikeus))
                  (save-results herate opiskeluoikeus koulutustoimija))))
            (do
              (log/info "No opiskeluoikeus found for oid"
                        (:opiskeluoikeus-oid herate))
              (log/info "Not saving heräte - hankkimistapa-id"
                        (:hankkimistapa-id herate)))))
        (catch JsonParseException e
          (log/error "Virhe viestin lukemisessa:" e))
        (catch ExceptionInfo e
          (if (and (:status (ex-data e))
                   (= 404 (:status (ex-data e))))
            (do
              (log/error "Ei opiskeluoikeutta"
                         (:opiskeluoikeus-oid
                          (parse-string (.getBody msg) true)))
              (log/error "Virhe:" e))
            (do (log/error e)
                (throw e))))))))
