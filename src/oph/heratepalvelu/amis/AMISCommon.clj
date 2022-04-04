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
  "Luo kyselylinkin herätteelle, tallentaa herätteen, ja lähettää tietoja
  kyselylinkistä ja herätteen tallentamisesta ehoksiin."
  [herate opiskeluoikeus koulutustoimija]
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
                          (:koodiarvo (:suorituskieli suoritus)))]
      (if (c/check-duplicate-herate? oppija
                                     koulutustoimija
                                     laskentakausi
                                     kyselytyyppi)
        (let [req-body (arvo/build-arvo-request-body
                         herate
                         opiskeluoikeus
                         uuid
                         koulutustoimija
                         suoritus
                         alkupvm
                         loppupvm)
              arvo-resp (if (= kyselytyyppi "aloittaneet")
                          (arvo/create-amis-kyselylinkki
                            req-body)
                          (arvo/create-amis-kyselylinkki-catch-404
                            req-body))]
          (if-let [kyselylinkki (:kysely_linkki arvo-resp)]
            (try
              (log/info "Tallennetaan kantaan" (str koulutustoimija "/" oppija)
                        (str kyselytyyppi "/" laskentakausi) ", request-id:"
                        uuid)
              (ddb/put-item
                {:toimija_oppija      [:s (str koulutustoimija "/" oppija)]
                 :tyyppi_kausi        [:s (str kyselytyyppi "/" laskentakausi)]
                 :kyselylinkki        [:s kyselylinkki]
                 :sahkoposti          [:s (:sahkoposti herate)]
                 :suorituskieli       [:s suorituskieli]
                 :lahetystila         [:s (:ei-lahetetty c/kasittelytilat)]
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
                 :tallennuspvm        [:s (str (c/local-date-now))]}
                {:cond-expr (str "attribute_not_exists(toimija_oppija) AND "
                                 "attribute_not_exists(tyyppi_kausi)")})
              (try
                (ehoks/add-kyselytunnus-to-hoks
                  (:ehoks-id herate)
                  {:kyselylinkki kyselylinkki
                   :tyyppi       kyselytyyppi
                   :alkupvm      alkupvm
                   :lahetystila  (:ei-lahetetty c/kasittelytilat)})
                (catch Exception e
                  (log/error "Virhe linkin lähetyksessä eHOKSiin " e)))
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
                   :opiskeluoikeus-oid    (:oid opiskeluoikeus)
                   :koulutuksenjarjestaja koulutustoimija
                   :tutkintotunnus        (get-in suoritus [:koulutusmoduuli
                                                            :tunniste
                                                            :koodiarvo])
                   :kyselytunnus          (last (str/split kyselylinkki #"/"))
                   :voimassa-loppupvm     loppupvm}))
              (catch ConditionalCheckFailedException _
                (log/warn "Tämän kyselyn linkki on jo toimituksessa oppilaalle"
                          oppija "koulutustoimijalla" koulutustoimija "(tyyppi"
                          kyselytyyppi "kausi" laskentakausi ")"
                          "Deaktivoidaan kyselylinkki, request-id" uuid)
                (arvo/delete-amis-kyselylinkki kyselylinkki))
              (catch AwsServiceException e
                (log/error "Virhe tietokantaan tallennettaessa"
                           kyselylinkki
                           uuid)
                (arvo/delete-amis-kyselylinkki kyselylinkki)
                (throw e))
              (catch Exception e
                (arvo/delete-amis-kyselylinkki kyselylinkki)
                (log/error "Unknown error " e)
                (throw e)))
            (log/error "Ei kyselylinkkiä arvon palautteessa" arvo-resp)))
        (try
          (if (= kyselytyyppi "aloittaneet")
            (ehoks/patch-amis-aloitusherate-kasitelty (:ehoks-id herate))
            (ehoks/patch-amis-paattoherate-kasitelty (:ehoks-id herate))))))))

(defn update-herate
  "Wrapper update-itemin ympäri, joka yksinkertaistaa herätteen päivitykset
  tietokantaan."
  [herate updates]
  (ddb/update-item
    {:toimija_oppija [:s (:toimija_oppija herate)]
     :tyyppi_kausi   [:s (:tyyppi_kausi herate)]}
    (c/create-update-item-options updates)
    (:herate-table env)))
