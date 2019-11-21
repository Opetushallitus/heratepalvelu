(ns oph.heratepalvelu.common
  (:require [clj-time.format :as f]
            [clj-time.coerce :as c]
            [clj-time.core :as t]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.organisaatio :refer [get-organisaatio]]
            [oph.heratepalvelu.external.arvo :refer :all]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.ehoks :refer [add-kyselytunnus-to-hoks]]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [schema.core :as s])
  (:import (java.util UUID)
           (software.amazon.awssdk.awscore.exception AwsServiceException)
           (software.amazon.awssdk.services.dynamodb.model ConditionalCheckFailedException)))

(s/defschema herate-schema
             {:ehoks-id           s/Num
              :kyselytyyppi       s/Str
              :opiskeluoikeus-oid s/Str
              :oppija-oid         s/Str
              :sahkoposti         s/Str
              :alkupvm            s/Str})

(defn generate-uuid []
  (.toString (UUID/randomUUID)))

(defn get-koulutustoimija-oid [opiskeluoikeus]
  (if-let [koulutustoimija-oid (:oid (:koulutustoimija opiskeluoikeus))]
    koulutustoimija-oid
    (do
      (log/info "Ei koulutustoimijaa opiskeluoikeudessa "
                (:oid opiskeluoikeus) ", haetaan Organisaatiopalvelusta")
      (:parentOid
        (get-organisaatio
          (get-in opiskeluoikeus [:oppilaitos :oid]))))))

(defn kausi [alkupvm]
  (let [[year month] (str/split alkupvm #"-")]
    (if (> (Integer/parseInt month) 6)
      (str year "-" (+ (Integer/parseInt year) 1))
      (str (- (Integer/parseInt year) 1) "-" year))))

(defn check-suoritus-type? [suoritus]
  (if (or (= (:koodiarvo (:tyyppi suoritus)) "ammatillinentutkinto")
          (= (:koodiarvo (:tyyppi suoritus)) "ammatillinentutkintoosittainen"))
    true
    (log/info "Väärä suoritustyyppi '"(:koodiarvo (:tyyppi suoritus))"'")))

(defn check-organisaatio-whitelist?
  ([koulutustoimija]
    (check-organisaatio-whitelist? koulutustoimija (c/to-long (t/today))))
  ([koulutustoimija timestamp]
   (let [item (ddb/get-item {:organisaatio-oid [:s koulutustoimija]}
                            (:orgwhitelist-table env))]
     (if
       (when (:kayttoonottopvm item)
         (<= (c/to-long (f/parse (:date f/formatters) (:kayttoonottopvm item)))
             (* 1000 timestamp)))
       true
       (log/info "Koulutustoimija " koulutustoimija " ei ole mukana automaatiossa")))))

(defn check-duplicate-herate? [oppija koulutustoimija laskentakausi kyselytyyppi]
  (if
    (empty? (ddb/get-item {:toimija_oppija [:s (str koulutustoimija "/" oppija)]
                           :tyyppi_kausi [:s (str kyselytyyppi "/" laskentakausi)]}))
    true
    (log/warn "Tämän kyselyn linkki on jo toimituksessa oppilaalle "
              oppija " koulutustoimijalla " koulutustoimija
              "(tyyppi '" kyselytyyppi "' kausi " laskentakausi ")")))

(def herate-checker
  (s/checker herate-schema))

(defn save-herate [herate opiskeluoikeus]
  (log/info "Kerätään tietoja " (:ehoks-id herate) " " (:kyselytyyppi herate))
  (if (some? (herate-checker herate))
    (log/error {:herate herate :msg (herate-checker herate)})
    (let [kyselytyyppi (:kyselytyyppi herate)
          alkupvm (:alkupvm herate)
          koulutustoimija (get-koulutustoimija-oid opiskeluoikeus)
          suoritus (first (seq (:suoritukset opiskeluoikeus)))
          oppija (str (:oppija-oid herate))
          laskentakausi (kausi alkupvm)
          uuid (generate-uuid)
          oppilaitos (:oid (:oppilaitos opiskeluoikeus))
          suorituskieli
          (str/lower-case (:koodiarvo (:suorituskieli suoritus)))]
      (when (check-duplicate-herate?
              oppija koulutustoimija laskentakausi kyselytyyppi)
        (if-let [kyselylinkki
                 (get-kyselylinkki
                   (build-arvo-request-body
                     herate
                     opiskeluoikeus
                     uuid
                     koulutustoimija))]
          (try
            (log/info "Tallennetaan kantaan " (str koulutustoimija "/" oppija)
                      " " (str kyselytyyppi "/" laskentakausi))
            (ddb/put-item {:toimija_oppija [:s (str koulutustoimija "/" oppija)]
                           :tyyppi_kausi [:s (str kyselytyyppi "/" laskentakausi)]
                           :kyselylinkki [:s kyselylinkki]
                           :sahkoposti [:s (:sahkoposti herate)]
                           :suorituskieli [:s suorituskieli]
                           :lahetystila [:s "ei_lahetetty"]
                           :alkupvm [:s alkupvm]
                           :request-id [:s uuid]
                           :oppilaitos [:s oppilaitos]
                           :ehoks-id [:n (str (:ehoks-id herate))]
                           :opiskeluoikeus-oid [:s (:oid opiskeluoikeus)]
                           :oppija-oid [:s oppija]
                           :koulutustoimija [:s koulutustoimija]
                           :kyselytyyppi [:s kyselytyyppi]
                           :rahoituskausi [:s laskentakausi]
                           :viestintapalvelu-id [:n "-1"]}
                          {:cond-expr (str "attribute_not_exists(oppija_toimija) AND "
                                           "attribute_not_exists(tyyppi_kausi)")})
            (add-kyselytunnus-to-hoks (:ehoks-id herate)
                                      {:kyselylinkki kyselylinkki
                                       :tyyppi kyselytyyppi
                                       :alkupvm alkupvm})
            (catch ConditionalCheckFailedException e
              (log/warn "Tämän kyselyn linkki on jo toimituksessa oppilaalle "
                        oppija " koulutustoimijalla " koulutustoimija
                        "(tyyppi " kyselytyyppi " kausi " (kausi alkupvm) ")"
                        "Deaktivoidaan kyselylinkki, request-id " uuid)
              (deactivate-kyselylinkki kyselylinkki))
            (catch AwsServiceException e
              (log/error "Virhe tietokantaan tallennettaessa " kyselylinkki " " uuid)
              (deactivate-kyselylinkki kyselylinkki)
              (throw e))
            (catch Exception e
              (deactivate-kyselylinkki kyselylinkki)
              (log/error "Unknown error " e)
              (throw e))))))))