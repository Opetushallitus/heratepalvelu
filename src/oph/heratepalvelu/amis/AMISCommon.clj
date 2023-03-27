(ns oph.heratepalvelu.amis.AMISCommon
  "Yhteiset funktiot AMIS-puolelle."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.external.ehoks :as ehoks])
  (:import (java.time LocalDate)
           (software.amazon.awssdk.awscore.exception AwsServiceException)
           (software.amazon.awssdk.services.dynamodb.model
             ConditionalCheckFailedException)))

(defn get-item-by-kyselylinkki
  "Hakee yhden herätteen tietokannasta kyselylinkin perusteella."
  [kyselylinkki]
  (try
    (first (ddb/query-items {:kyselylinkki [:eq [:s kyselylinkki]]}
                            {:index "resendIndex"}))
    (catch AwsServiceException e
      (log/error "Hakuvirhe (get-item-by-kyselylinkki)" kyselylinkki ":" e)
      (throw e))))

(defn save-herate
  "Tarkistaa herätteen ja tallentaa sen tietokantaan."
  [herate opiskeluoikeus koulutustoimija herate-source]
  (log/info "Kerätään tietoja " (:ehoks-id herate) " " (:kyselytyyppi herate))
  (if (some? (c/herate-checker herate))
    (log/error {:herate herate :msg (c/herate-checker herate)})
    (let [kyselytyyppi (:kyselytyyppi herate)
          heratepvm (:alkupvm herate)
          herate-date (LocalDate/parse heratepvm)
          alku-date (c/alku herate-date)
          alkupvm   (str alku-date)
          loppu-date (c/loppu herate-date alku-date)
          loppupvm  (str loppu-date)
          suoritus (c/get-suoritus opiskeluoikeus)
          oppija (str (:oppija-oid herate))
          laskentakausi (c/kausi heratepvm)
          uuid (c/generate-uuid)
          oppilaitos (:oid (:oppilaitos opiskeluoikeus))
          suorituskieli (str/lower-case
                          (:koodiarvo (:suorituskieli suoritus)))
          rahoitusryhma (c/get-rahoitusryhma opiskeluoikeus
                                             herate-date)]
      (if (c/check-duplicate-herate? oppija
                                     koulutustoimija
                                     laskentakausi
                                     kyselytyyppi
                                     herate-source)
        ; Vaikka Arvokyselyä ei tässä tehdä, body-objektia käytetään muuten.
        (let [req-body (arvo/build-arvo-request-body
                         herate
                         opiskeluoikeus
                         uuid
                         koulutustoimija
                         suoritus
                         alkupvm
                         loppupvm)
              db-data
              {:toimija_oppija      [:s (str koulutustoimija "/" oppija)]
               :tyyppi_kausi        [:s (str kyselytyyppi "/" laskentakausi)]
               :sahkoposti          [:s (:sahkoposti herate)]
               :suorituskieli       [:s suorituskieli]
               :lahetystila         [:s (:ei-lahetetty c/kasittelytilat)]
               :sms-lahetystila     [:s (if (or
                                              (= kyselytyyppi
                                                 "tutkinnon_suorittaneet")
                                              (= kyselytyyppi
                                                 "tutkinnon_osia_suorittaneet"))
                                          (:ei-lahetetty c/kasittelytilat)
                                          (:ei-laheteta c/kasittelytilat))]
               :alkupvm             [:s alkupvm]
               :heratepvm           [:s heratepvm]
               :request-id          [:s uuid]
               :oppilaitos          [:s oppilaitos]
               :ehoks-id            [:n (str (:ehoks-id herate))]
               :opiskeluoikeus-oid  [:s (:oid opiskeluoikeus)]
               :oppija-oid          [:s oppija]
               :koulutustoimija     [:s koulutustoimija]
               :kyselytyyppi        [:s kyselytyyppi]
               :rahoituskausi       [:s laskentakausi]
               :viestintapalvelu-id [:n "-1"]
               :voimassa-loppupvm   [:s loppupvm]
               :tutkintotunnus      [:s (str (:tutkintotunnus req-body))]
               :osaamisala          [:s (str (seq (:osaamisala req-body)))]
               :toimipiste-oid      [:s (str (:toimipiste_oid req-body))]
               :hankintakoulutuksen-toteuttaja
               [:s (str (:hankintakoulutuksen_toteuttaja req-body))]
               :tallennuspvm        [:s (str (c/local-date-now))]
               :rahoitusryhma       [:s rahoitusryhma]
               :herate-source       [:s herate-source]}
              db-data-cond-values
              (cond-> db-data
                (not-empty (:puhelinnumero herate))
                (assoc :puhelinnumero [:s (:puhelinnumero herate)]))]
          (try
            (log/info "Tallennetaan kantaan" (str koulutustoimija "/" oppija)
                      (str kyselytyyppi "/" laskentakausi) ", request-id:"
                      uuid)

            (ddb/put-item
              db-data-cond-values
              (if (= herate-source (:ehoks c/herate-sources))
                {:cond-expr "attribute_not_exists(kyselylinkki)"}
                {:cond-expr (str "attribute_not_exists(toimija_oppija) AND "
                                 "attribute_not_exists(tyyppi_kausi) OR "
                                 "attribute_not_exists(kyselylinkki) AND "
                                 "#source = :koski")
                 :expr-attr-names {"#source" "herate-source"}
                 :expr-attr-vals {":koski" [:s (:koski c/herate-sources)]}}))
            (c/delete-other-paattoherate oppija
                                         koulutustoimija
                                         laskentakausi
                                         kyselytyyppi)
            (try
              (if (= kyselytyyppi "aloittaneet")
                (ehoks/patch-amis-aloitusherate-kasitelty (:ehoks-id herate))
                (ehoks/patch-amis-paattoherate-kasitelty (:ehoks-id herate)))
              (catch Exception e
                (log/error
                  "Virhe käsittelytilan päivittämisessä eHOKS-palveluun")))
            (when (c/has-nayttotutkintoonvalmistavakoulutus? opiskeluoikeus)
              (log/info
                {:nayttotutkinto        true
                 :hoks-id               (:ehoks-id herate)
                 :request-id            uuid
                 :opiskeluoikeus-oid    (:oid opiskeluoikeus)
                 :koulutuksenjarjestaja koulutustoimija
                 :tutkintotunnus        (get-in suoritus [:koulutusmoduuli
                                                          :tunniste
                                                          :koodiarvo])
                 :voimassa-loppupvm     loppupvm}))

            (catch ConditionalCheckFailedException e
              (log/warn "Estetty ylikirjoittamasta olemassaolevaa herätettä."
                        "Oppija:" oppija "koulutustoimijalla:" koulutustoimija
                        "tyyppi:" kyselytyyppi "kausi:" laskentakausi
                        "request-id:" uuid "Conditional check exception:" e))
            (catch AwsServiceException e
              (log/error "Virhe tietokantaan tallennettaessa" uuid)
              (throw e))
            (catch Exception e
              (log/error "Unknown error " e)
              (throw e))))))))

(defn update-herate
  "Wrapper update-itemin ympäri, joka yksinkertaistaa herätteen päivitykset
  tietokantaan."
  [herate updates]
  (ddb/update-item
    {:toimija_oppija [:s (:toimija_oppija herate)]
     :tyyppi_kausi   [:s (:tyyppi_kausi herate)]}
    (c/create-update-item-options updates)
    (:herate-table env)))
