(ns oph.heratepalvelu.common
  (:require [clj-time.format :as f]
            [clj-time.coerce :as c]
            [clj-time.core :as t]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.organisaatio :refer [get-organisaatio]]
            [oph.heratepalvelu.external.arvo :refer :all]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]])
  (:import (java.util UUID)
           (software.amazon.awssdk.awscore.exception AwsServiceException)
           (software.amazon.awssdk.services.dynamodb.model ConditionalCheckFailedException)))

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
             timestamp))
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

(defn get-vahvistus-pvm [opiskeluoikeus]
  (if-let [vahvistus-pvm (-> (:suoritukset opiskeluoikeus)
                             (seq)
                             (first)
                             (:vahvistus)
                             (:päivä))]
    vahvistus-pvm
    (log/warn "Opiskeluoikeudessa" (:oid opiskeluoikeus)
              "ei vahvistus päivämäärää")))

(defn save-herate [hoks opiskeluoikeus kyselytyyppi heratepvm]
  (log/info "Kerätään tietoja " (:ehoks-id hoks) " " (:kyselytyyppi hoks))
  (let [koulutustoimija (get-koulutustoimija-oid opiskeluoikeus)
        suoritus (first (seq (:suoritukset opiskeluoikeus)))
        oppija (str (:oppija-oid hoks))
        laskentakausi (kausi heratepvm)
        uuid (generate-uuid)
        oppilaitos (:oid (:oppilaitos opiskeluoikeus))
        tutkinto
        (:koodiarvo (:tunniste (:koulutusmoduuli suoritus)))
        suorituskieli
        (str/lower-case (:koodiarvo (:suorituskieli suoritus)))]
    (when (check-duplicate-herate?
            oppija koulutustoimija laskentakausi kyselytyyppi)
      (if-let [kyselylinkki
               (get-kyselylinkki
                 (build-arvo-request-body
                   heratepvm
                   uuid
                   kyselytyyppi
                   koulutustoimija
                   oppilaitos
                   tutkinto
                   suorituskieli))]
        (try
          (log/info "Tallennetaan kantaan " (str koulutustoimija "/" oppija)
                    " " (str kyselytyyppi "/" laskentakausi))
          (ddb/put-item {:toimija_oppija [:s (str koulutustoimija "/" oppija)]
                         :tyyppi_kausi [:s (str kyselytyyppi "/" laskentakausi)]
                         :kyselylinkki [:s kyselylinkki]
                         :sahkoposti [:s (:sahkoposti hoks)]
                         :suorituskieli [:s suorituskieli]
                         :lahetystila [:s "ei_lahetetty"]
                         :alkupvm [:s heratepvm]
                         :request-id [:s uuid]
                         :oppilaitos [:s oppilaitos]
                         :ehoks-id [:n (str (:ehoks-id hoks))]
                         :opiskeluoikeus-oid [:s (:oid opiskeluoikeus)]
                         :oppija-oid [:s oppija]
                         :koulutustoimija [:s koulutustoimija]
                         :kyselytyyppi [:s kyselytyyppi]
                         :rahoituskausi [:s laskentakausi]
                         :viestintapalvelu-id -1}
                        {:cond-expr "attribute_not_exists(oppija_toimija) AND attribute_not_exists(tyyppi_kausi)"})
          (catch ConditionalCheckFailedException e
            (log/warn "Tämän kyselyn linkki on jo toimituksessa oppilaalle "
                      oppija " koulutustoimijalla " koulutustoimija
                      "(tyyppi " kyselytyyppi " kausi " (kausi heratepvm))
            (deactivate-kyselylinkki kyselylinkki))
          (catch AwsServiceException e
            (log/error "Virhe tietokantaan tallennettaessa " kyselylinkki " " uuid)
            (deactivate-kyselylinkki kyselylinkki)
            (throw e))
          (catch Exception e
            (deactivate-kyselylinkki kyselylinkki)
            (throw e)))))))