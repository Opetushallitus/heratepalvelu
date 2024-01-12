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

(defn get-herate-by-kyselylinkki!
  "Hakee yhden herätteen tietokannasta kyselylinkin perusteella."
  [kyselylinkki]
  (try
    (first (ddb/query-items {:kyselylinkki [:eq [:s kyselylinkki]]}
                            {:index "resendIndex"}))
    (catch AwsServiceException e
      (log/error "Hakuvirhe (get-item-by-kyselylinkki)" kyselylinkki ":" e)
      (throw e))))

(defn save-herate-ddb!
  "Vie herätteen tietokantaan ja poistaa mahdollisen aiemman vastaavan."
  [db-data herate-source oppija koulutustoimija laskentakausi kyselytyyppi]
  (try
    (ddb/put-item
      db-data
      (if (= herate-source (:ehoks c/herate-sources))
        {:cond-expr "attribute_not_exists(kyselylinkki)"}
        {:cond-expr (str "attribute_not_exists(toimija_oppija) AND "
                         "attribute_not_exists(tyyppi_kausi) OR "
                         "attribute_not_exists(kyselylinkki) AND "
                         "#source = :koski")
         :expr-attr-names {"#source" "herate-source"}
         :expr-attr-vals {":koski" [:s (:koski c/herate-sources)]}}))
    (c/delete-other-paattoherate
      oppija koulutustoimija laskentakausi kyselytyyppi)
    (catch ConditionalCheckFailedException e
      (log/warn e "Estetty ylikirjoittamasta olemassaolevaa herätettä."
                "Oppija:" oppija "koulutustoimijalla:" koulutustoimija
                "tyyppi:" kyselytyyppi "kausi:" laskentakausi))
    (catch Exception e
      (log/error e "at save-herate-ddb!")
      (throw e))))

(defn update-herate-ehoks!
  "Vie eHOKSiin tiedon siitä, että heräte on käsitelty."
  [ehoks-id kyselytyyppi]
  (try
    (if (= kyselytyyppi "aloittaneet")
      (ehoks/patch-amis-aloitusherate-kasitelty ehoks-id)
      (ehoks/patch-amis-paattoherate-kasitelty ehoks-id))
    (catch Exception e (log/error e "at update-herate-ehoks!"))))

(defn check-and-save-herate!
  "Tarkistaa herätteen ja tallentaa sen tietokantaan."
  [herate opiskeluoikeus koulutustoimija herate-source]
  (log/info "Kerätään tietoja " (:ehoks-id herate) " " (:kyselytyyppi herate))
  (if-some [errors (c/herate-schema-errors herate)]
    (log/error "Heräte" herate "ei vastaa skeemaa:" errors)
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
      (if-some [existing (c/already-superseding-herate! oppija
                                                        koulutustoimija
                                                        laskentakausi
                                                        kyselytyyppi
                                                        herate-source)]
        (log/info "Meillä on jo vastaava heräte" existing
                  "joten ei ylikirjoiteta. Uudessa oppija:" oppija
                  "koulutustoimija:" koulutustoimija
                  ";" kyselytyyppi laskentakausi)

        ; Vaikka Arvokyselyä ei tässä tehdä, body-objektia käytetään muuten.
        (let [req-body (arvo/build-arvo-request-body
                         herate
                         opiskeluoikeus
                         uuid
                         koulutustoimija
                         suoritus
                         alkupvm
                         loppupvm
                         nil)
              db-data
              {:toimija_oppija      [:s (str koulutustoimija "/" oppija)]
               :tyyppi_kausi        [:s (str kyselytyyppi "/" laskentakausi)]
               :suorituskieli       [:s suorituskieli]
               :lahetystila         [:s (if (not-empty (:sahkoposti herate))
                                          (:ei-lahetetty c/kasittelytilat)
                                          (:ei-laheteta c/kasittelytilat))]
               :sms-lahetystila
               [:s (if (and
                         (or (= kyselytyyppi "tutkinnon_suorittaneet")
                             (= kyselytyyppi "tutkinnon_osia_suorittaneet"))
                         (not-empty (:puhelinnumero herate)))
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
               :herate-source       [:s herate-source]}
              db-data-cond-values
              (cond-> db-data
                (not-empty (:sahkoposti herate))
                (assoc :sahkoposti [:s (:sahkoposti herate)])
                (not-empty (:puhelinnumero herate))
                (assoc :puhelinnumero [:s (:puhelinnumero herate)]))]
          (log/info "Tallennetaan kantaan" (str koulutustoimija "/" oppija)
                    (str kyselytyyppi "/" laskentakausi) ", request-id:"
                    uuid)
          (save-herate-ddb! db-data-cond-values herate-source
                            oppija koulutustoimija laskentakausi kyselytyyppi)
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
               :voimassa-loppupvm     loppupvm})))))))

(defn update-herate
  "Wrapper update-itemin ympäri, joka yksinkertaistaa herätteen päivitykset
  tietokantaan."
  [herate updates]
  (ddb/update-item
    {:toimija_oppija [:s (:toimija_oppija herate)]
     :tyyppi_kausi   [:s (:tyyppi_kausi herate)]}
    (c/create-update-item-options updates)
    (:herate-table env)))
