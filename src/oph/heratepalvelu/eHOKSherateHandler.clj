(ns oph.heratepalvelu.eHOKSherateHandler
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [clojure.string :refer [split]]
            [oph.heratepalvelu.external.koski :refer [get-opiskeluoikeus]]
            [oph.heratepalvelu.external.arvo :refer :all]
            [oph.heratepalvelu.external.organisaatio :refer [get-organisaatio]]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [schema.core :as s])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)
           (software.amazon.awssdk.services.dynamodb.model ConditionalCheckFailedException)
           (com.fasterxml.jackson.core JsonParseException)))

(gen-class
  :name "oph.heratepalvelu.eHOKSherateHandler"
  :methods [[^:static handleHOKShyvaksytty [com.amazonaws.services.lambda.runtime.events.SQSEvent] void]])

(s/defschema herate-msg
  {:ehoks-id s/Num
   :kyselytyyppi s/Str
   :opiskeluoikeus-oid s/Str
   :oppija-oid s/Str
   :sahkoposti s/Str
   :ensikertainen-hyvaksyminen s/Str})

(defn generate-uuid []
  (.toString (java.util.UUID/randomUUID)))

(defn get-koulutustoimija-oid [opiskeluoikeus]
  (if-let [koulutustoimija-oid (:oid (:koulutustoimija opiskeluoikeus))]
    koulutustoimija-oid
    ((log/info "Ei koulutustoimijaa opiskeluoikeudessa " (:oid opiskeluoikeus) ", haetaan Organisaatiopalvelusta")
     (-> opiskeluoikeus
         :oppilaitos
         :oid
         get-organisaatio
         :parentOid))))

(defn kausi [alkupvm]
  (let [[year month] (split alkupvm #"-")]
    (if (> (Integer/parseInt month) 6)
      (str year "-" (+ (Integer/parseInt year) 1))
      (str (- (Integer/parseInt year) 1) "-" year))))

(defn- save-HOKSHyvaksytty [hoks]
  (log/info "Kerätään tietoja " (:ehoks-id hoks) " " (:kyselytyyppi hoks))
  (let [kyselytyyppi (:kyselytyyppi hoks)
        opiskeluoikeus (get-opiskeluoikeus (:opiskeluoikeus-oid hoks))
        oppilaitos (:oid (:oppilaitos opiskeluoikeus))
        koulutustoimija (get-koulutustoimija-oid opiskeluoikeus)
        suoritukset (seq (:suoritukset opiskeluoikeus))
        tutkinto (:koodiarvo (:tunniste (:koulutusmoduuli (first suoritukset))))
        suorituskieli (:koodiarvo (:suorituskieli (first suoritukset)))
        alkupvm (:ensikertainen-hyvaksyminen hoks)
        oppija (str (:oppija-oid hoks))
        uuid (generate-uuid)
        laskentakausi (kausi alkupvm)
        kyselylinkki (create-kyselylinkki (build-arvo-request-body alkupvm
                                                                   uuid
                                                                   kyselytyyppi
                                                                   koulutustoimija
                                                                   oppilaitos
                                                                   tutkinto
                                                                   suorituskieli))]
    (log/info "Tallennetaan kantaan")
    (try
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
        ;Lambdan suoritus päättyy onnistuneesti
        (log/warn "Tämän kyselyn linkki on jo toimituksessa oppilaalle "
                  oppija " koulutustoimijalla " koulutustoimija
                  "(tyyppi " kyselytyyppi " kausi " (kausi alkupvm))
        (deactivate-kyselylinkki kyselylinkki))
      (catch AwsServiceException e
        (log/error "Virhe tietokantaan tallennettaessa")
        (deactivate-kyselylinkki kyselylinkki)
        (throw e)))))

(defn -handleHOKShyvaksytty [this event]
  (let [messages (seq (.getRecords event))]
    (doseq [msg messages]
      (try
        (let [hoks (parse-string (.getBody msg) true)]
          (if (nil? (s/check herate-msg hoks))
            (save-HOKSHyvaksytty hoks)
            (log/error (s/check herate-msg hoks))))
        (catch JsonParseException e
          (log/error "Virheellinen viesti " e))))))
