(ns oph.heratepalvelu.eHOKSherateHandler
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.external.koski :refer [get-opiskeluoikeus]]
            [oph.heratepalvelu.external.arvo :refer [get-kyselylinkki build-arvo-request-body]]
            [oph.heratepalvelu.external.organisaatio :refer [get-organisaatio]]
            [oph.heratepalvelu.db.dynamodb :as ddb]))

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

(defn -handleHOKShyvaksytty [this event]
  (let [messages (seq (.getRecords event))]
    (doseq [msg messages]
      (let [hoks (parse-string (.getBody msg) true)
            kyselytyyppi "HOKS_hyvaksytty"]
        (log/info "Gathering data for " (:id hoks) " " kyselytyyppi)
        (let [opiskeluoikeus (get-opiskeluoikeus (:opiskeluoikeus-oid hoks))
              koulutustoimija (get-koulutustoimija-oid opiskeluoikeus)
              uuid (generate-uuid)
              kyselylinkki (get-kyselylinkki (build-arvo-request-body hoks
                                                                      opiskeluoikeus
                                                                      koulutustoimija
                                                                      (:ensikertainen-hyvaksynta hoks)
                                                                      uuid
                                                                      kyselytyyppi))]
          (ddb/put-item {:uuid uuid
                         :kyselytyyppi kyselytyyppi
                         :kyselylinkki kyselylinkki
                         :sahkoposti (:sahkoposti hoks)
                         :lahetetty false
                         :koulutuksenjarjestaja koulutuksenjarjestaja
                         :oppilaitos (:oid (:oppilaitos opiskeluoikeus))
                         :oppija-oid (:oppija-oid hoks)
                         :ehoks-id (:id hoks)
                         :opiskeluoikeus-oid (:opiskeluoikeus-oid hoks)}))))))
