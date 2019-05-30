(ns oph.heratepalvelu.eHOKSherateHandler
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [clojure.string :refer [split]]
            [oph.heratepalvelu.external.koski :refer [get-opiskeluoikeus]]
            [oph.heratepalvelu.external.arvo :refer :all]
            [oph.heratepalvelu.external.organisaatio :refer [get-organisaatio]]
            [oph.heratepalvelu.db.dynamodb :as ddb])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)
           (software.amazon.awssdk.services.dynamodb.model ConditionalCheckFailedException)))

(gen-class
  :name "oph.heratepalvelu.eHOKSherateHandler"
  :methods [[^:static handleHOKShyvaksytty [com.amazonaws.services.lambda.runtime.events.SQSEvent] void]])

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
  (log/info "kausi " alkupvm)
  (let [[year month] (split alkupvm #"-")]
    (if (> (Integer/parseInt month) 6)
      (str year "-" (+ (Integer/parseInt year) 1))
      (str (- (Integer/parseInt year) 1) "-" year))))

(defn -handleHOKShyvaksytty [this event]
  (let [messages (seq (.getRecords event))]
    (doseq [msg messages]
      (let [hoks (parse-string (.getBody msg) true)
            kyselytyyppi (:kyselytyyppi hoks)]
        (log/info "Kerätään tietoja " (:ehoks-id hoks) " " kyselytyyppi)
        (let [opiskeluoikeus (get-opiskeluoikeus (:opiskeluoikeus-oid hoks))
              oppilaitos (:oid (:oppilaitos opiskeluoikeus))
              koulutustoimija (get-koulutustoimija-oid opiskeluoikeus)
              suoritukset (seq (:suoritukset opiskeluoikeus))
              tutkinto (:koodiarvo (:tunniste (:koulutusmoduuli (first suoritukset))))
              suorituskieli "fi"                                ;(:lyhytNimi (:suorituskieli (first suoritukset)))
              alkupvm (:ensikertainen-hyvaksyminen hoks)
              oppija (str (:oppija-oid hoks))
              uuid (generate-uuid)
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
                           :tyyppi_kausi [:s (str kyselytyyppi "/" (kausi alkupvm))]
                           :kyselylinkki [:s kyselylinkki]
                           :sahkoposti [:s (:sahkoposti hoks)]
                           :suorituskieli [:s suorituskieli]
                           :lahetystila [:s "ei_lahetetty"]
                           :alkupvm [:s alkupvm]
                           :request-id [:s uuid]
                           :oppilaitos [:s oppilaitos]
                           :ehoks-id [:n (str (:ehoks-id hoks))]
                           :opiskeluoikeus-oid [:s (:opiskeluoikeus-oid hoks)]}
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
              (throw e))))))))
