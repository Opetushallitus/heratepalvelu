(ns oph.heratepalvelu.eHOKSherateHandler
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [clojure.string :refer [split]]
            [oph.heratepalvelu.external.koski :refer [get-opiskeluoikeus]]
            [oph.heratepalvelu.external.arvo :refer :all]
            [oph.heratepalvelu.external.organisaatio :refer [get-organisaatio]]
            [oph.heratepalvelu.db.dynamodb :as ddb])
  (:import (com.amazonaws.services.dynamodbv2.model ConditionalCheckFailedException)
           (com.amazonaws AmazonServiceException)))

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
            kyselytyyppi "HOKS_hyvaksytty"]
        (log/info "Kerätään tietoja " (:id hoks) " " kyselytyyppi)
        (let [opiskeluoikeus (get-opiskeluoikeus (:opiskeluoikeus-oid hoks))
              oppilaitos (:oid (:oppilaitos opiskeluoikeus))
              koulutustoimija (get-koulutustoimija-oid opiskeluoikeus)
              suoritukset (seq (:suoritukset opiskeluoikeus))
              tutkinto (:koodiarvo (:tunniste (:koulutusmoduuli (first suoritukset))))
              suorituskieli (:lyhytNimi (:suorituskieli (first suoritukset)))
              alkupvm (:ensikertainen-hyvaksyminen hoks)
              oppija (str (:oppija-oid hoks))
              uuid (generate-uuid)
              kyselylinkki (get-kyselylinkki (build-arvo-request-body alkupvm
                                                                      uuid
                                                                      kyselytyyppi
                                                                      koulutustoimija
                                                                      oppilaitos
                                                                      tutkinto
                                                                      suorituskieli))]
          (log/info "Tallennetaan kantaan")
          (try
            (ddb/put-item {:oppija_toimija (str oppija "_" koulutustoimija)
                           :tyyppi_kausi (str kyselytyyppi "_" (kausi alkupvm))
                           :kyselylinkki kyselylinkki
                           :sahkoposti (:sahkoposti hoks)
                           :suorituskieli suorituskieli
                           :lahetystila "ei_lahetetty"
                           :alkupvm alkupvm
                           :request-id uuid
                           :oppilaitos oppilaitos
                           :ehoks-id (:id hoks)
                           :opiskeluoikeus-oid (:opiskeluoikeus-oid hoks)}
                          {:cond-expr "attribute_not_exists(oppija_toimija) AND attribute_not_exists(tyyppi_kausi)"})
            (catch ConditionalCheckFailedException e
              (log/warn "Tämän kyselyn linkki on jo toimituksessa oppilaalle "
                        oppija " koulutustoimijalla " koulutustoimija)
              (deactivate-kyselylinkki kyselylinkki))
            (catch AmazonServiceException e
              (log/error "Virhe tietokantaan tallennettaessa")
              (deactivate-kyselylinkki kyselylinkki)
              (throw e))))))))
