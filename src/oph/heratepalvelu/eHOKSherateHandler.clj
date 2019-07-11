(ns oph.heratepalvelu.eHOKSherateHandler
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [clojure.string :refer [split lower-case]]
            [oph.heratepalvelu.external.koski :refer [get-opiskeluoikeus]]
            [oph.heratepalvelu.external.arvo :refer :all]
            [oph.heratepalvelu.external.organisaatio :refer [get-organisaatio]]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [clj-time.core :refer [today before?]]
            [clj-time.format :as f]
            [clj-time.coerce :as c]
            [environ.core :refer [env]]
            [schema.core :as s])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)
           (software.amazon.awssdk.services.dynamodb.model ConditionalCheckFailedException)
           (com.fasterxml.jackson.core JsonParseException)
           (clojure.lang ExceptionInfo)))

(gen-class
  :name "oph.heratepalvelu.eHOKSherateHandler"
  :methods [[^:static handleHOKSherate [com.amazonaws.services.lambda.runtime.events.SQSEvent] void]])

(s/defschema herate-schema
  {:ehoks-id s/Num
   :kyselytyyppi s/Str
   :opiskeluoikeus-oid s/Str
   :oppija-oid s/Str
   :sahkoposti s/Str
   :alkupvm s/Str})

(defn generate-uuid []
  (.toString (java.util.UUID/randomUUID)))

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
  (let [[year month] (split alkupvm #"-")]
    (if (> (Integer/parseInt month) 6)
      (str year "-" (+ (Integer/parseInt year) 1))
      (str (- (Integer/parseInt year) 1) "-" year))))

(defn check-suoritus-type [suoritus]
  (or (= (:koodiarvo (:tyyppi suoritus)) "ammatillinentutkinto")
      (= (:koodiarvo (:tyyppi suoritus)) "ammatillinentutkintoosittainen")))

(defn check-organisaatio-whitelist [koulutustoimija]
  (let [item (ddb/get-item {:organisaatio-oid [:s koulutustoimija]}
                           (:orgwhitelist-table env))]
    (when (:kayttoonottopvm item)
      (<= (c/to-long (f/parse (:date f/formatters) (:kayttoonottopvm item)))
          (c/to-long (today))))))

(defn save-HOKS-herate [hoks]
  (log/info "Kerätään tietoja " (:ehoks-id hoks) " " (:kyselytyyppi hoks))
  (let [kyselytyyppi (:kyselytyyppi hoks)
        opiskeluoikeus (get-opiskeluoikeus (:opiskeluoikeus-oid hoks))
        oppilaitos (:oid (:oppilaitos opiskeluoikeus))
        koulutustoimija (get-koulutustoimija-oid opiskeluoikeus)
        suoritukset (seq (:suoritukset opiskeluoikeus))
        tutkinto (:koodiarvo (:tunniste (:koulutusmoduuli (first suoritukset))))
        suorituskieli (lower-case (:koodiarvo (:suorituskieli (first suoritukset))))
        alkupvm (:alkupvm hoks)
        oppija (str (:oppija-oid hoks))
        uuid (generate-uuid)
        laskentakausi (kausi alkupvm)]

    (if (and (check-suoritus-type (first suoritukset))
             (check-organisaatio-whitelist koulutustoimija))
      (if-let [item (ddb/get-item {:toimija_oppija [:s (str koulutustoimija "/" oppija)]
                                     :tyyppi_kausi [:s (str kyselytyyppi "/" laskentakausi)]})]
        (log/warn "Tämän kyselyn linkki on jo toimituksessa oppilaalle "
                  oppija " koulutustoimijalla " koulutustoimija
                  "(tyyppi " kyselytyyppi " kausi " (kausi alkupvm))
        (let [kyselylinkki
              (get-kyselylinkki
                (build-arvo-request-body alkupvm
                                         uuid
                                         kyselytyyppi
                                         koulutustoimija
                                         oppilaitos
                                         tutkinto
                                         suorituskieli))]
          (when kyselylinkki
            (try
              (log/info "Tallennetaan kantaan " (str koulutustoimija "/" oppija)
                        " " (str kyselytyyppi "/" laskentakausi))
              (ddb/put-item {:toimija_oppija [:s (str koulutustoimija "/" oppija)]
                             :tyyppi_kausi [:s (str kyselytyyppi "/" laskentakausi)]
                             :kyselylinkki [:s kyselylinkki]
                             :sahkoposti [:s (:sahkoposti hoks)]
                             :suorituskieli [:s suorituskieli]
                             :lahetystila [:s "ei_lahetetty"]
                             :alkupvm [:s alkupvm]
                             :request-id [:s uuid]
                             :oppilaitos [:s oppilaitos]
                             :ehoks-id [:n (str (:ehoks-id hoks))]
                             :opiskeluoikeus-oid [:s (:opiskeluoikeus-oid hoks)]
                             :oppija-oid [:s oppija]
                             :koulutustoimija [:s koulutustoimija]
                             :kyselytyyppi [:s kyselytyyppi]
                             :rahoituskausi [:s laskentakausi]}
                            {:cond-expr "attribute_not_exists(oppija_toimija) AND attribute_not_exists(tyyppi_kausi)"})
              (catch ConditionalCheckFailedException e
                (log/warn "Tämän kyselyn linkki on jo toimituksessa oppilaalle "
                          oppija " koulutustoimijalla " koulutustoimija
                          "(tyyppi " kyselytyyppi " kausi " (kausi alkupvm))
                (deactivate-kyselylinkki kyselylinkki))
              (catch AwsServiceException e
                (log/error "Virhe tietokantaan tallennettaessa " kyselylinkki " " uuid)
                (deactivate-kyselylinkki kyselylinkki)
                (throw e))))))
      (log/info "Väärä suoritustyyppi '" (:koodiarvo (:tyyppi (first suoritukset)))
                "' tai koulutustoimija " koulutustoimija " ei ole mukana automaatiossa"))))

(defn -handleHOKSherate [this event]
  (let [messages (seq (.getRecords event))]
    (doseq [msg messages]
      (try
        (let [hoks (parse-string (.getBody msg) true)]
          (if (nil? (s/check herate-schema hoks))
            (save-HOKS-herate hoks)
            (log/error (s/check herate-schema hoks))))
        (catch JsonParseException e
          (log/error "Virheellinen viesti " e))
        (catch ExceptionInfo e
          (if (and (> 399 (:status e))
                   (< 500 (:status)))
            (log/error "Unhandled client error " e)
            (throw e)))))))
