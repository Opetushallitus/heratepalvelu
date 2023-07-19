(ns oph.heratepalvelu.amis.AMISSMSHandler
  "Käsittelee SMS-viestien lähettämistä herätteitä varten."
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.amis.AMISCommon :as ac]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.elisa :as elisa]
            [oph.heratepalvelu.log.caller-log :as cl]
            [oph.heratepalvelu.external.organisaatio :as org])
  (:import (clojure.lang ExceptionInfo)
           (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.amis.AMISSMSHandler"
  :methods [[^:static handleAMISSMS
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn query-lahetettavat
  "Hakee eniten limit herätettä tietokannasta, joilta SMS-viestiä ei ole vielä
  lähetetty ja herätepäivämäärä on jo mennyt. Hakee vain herätteet, joihin
  kyselylinkki on jo luotu."
  []
  (ddb/query-items-with-expression
    "#smstila = :tila AND #alku <= :pvm"
    {:index "smsIndex"
     :filter-expression "attribute_exists(#linkki)"
     :expr-attr-names {"#smstila" "sms-lahetystila"
                       "#alku" "alkupvm"
                       "#linkki" "kyselylinkki"}
     :expr-attr-vals {":tila" [:s (:ei-lahetetty c/kasittelytilat)]
                      ":pvm" [:s (str (c/local-date-now))]}}
    (:herate-table env)))

(defn update-status-in-db!
  "Päivittää tekstiviestilähetyksen statuksen tietokantaan."
  [herate status]
  (log/info "Viedään tietokantaan status:" status)
  (let [update
        (cond-> {:sms-lahetyspvm    [:s (str (c/local-date-now))]
                 :sms-lahetystila   [:s (:status status)]}
          (not (#{(:vastausaika-loppunut c/kasittelytilat)
                  (:phone-invalid c/kasittelytilat)} (:status status)))
          (assoc :lahetettynumeroon
                 [:s (or (:converted status) (:puhelinnumero herate))]))]
    (try
      (ac/update-herate herate update)
      ;; TODO uuden alkupvm asettaminen
      (catch Exception e
        (log/error e "at update-status-in-db!")))))

(defn send-sms-and-return-status!
  "Lähettää SMS-viestin yhdelle herätteelle."
  [herate]
  (log/info "Käsitellään heräte:" herate)
  (cond
    (not (c/has-time-to-answer? (:voimassa-loppupvm herate)))
    {:status (:vastausaika-loppunut c/kasittelytilat)}

    (or (not (:puhelinnumero herate))
        (not (c/valid-number? (:puhelinnumero herate))))
    {:status (:phone-invalid c/kasittelytilat)}

    :else
    (try
      (let [numero (:puhelinnumero herate)
            oppilaitos (org/get-organisaatio (:oppilaitos herate))
            oppilaitos-nimi (:fi (:nimi oppilaitos))
            body (elisa/amis-msg-body (:kyselylinkki herate) oppilaitos-nimi)
            resp (elisa/send-sms numero body)
            results (get-in resp [:body :messages (keyword numero)])]
        results)
      (catch ExceptionInfo e
        (let [error-type (if (c/client-error? e) "Client" "Server")]
          (log/error e error-type "error while sending AMIS SMS")
          {:status (:ei-lahetetty c/kasittelytilat)}))
      (catch Exception e
        (log/error e "Virhe AMIS SMS-lähetyksessä")
        {:status (:ei-lahetetty c/kasittelytilat)}))))

(defn -handleAMISSMS
  "Hakee tietokannasta herätteitä, joilta SMS-viesti ei ole vielä lähetetty, ja
  käsittelee viestien lähetystä."
  [_ event ^com.amazonaws.services.lambda.runtime.Context context]
  (cl/log-caller-details-scheduled "AMISSMSHandler" event context)
  (let [lahetettavat (filter (c/time-left? context 60000) (query-lahetettavat))]
    (when (seq lahetettavat)
      (doseq [herate lahetettavat]
        (->> herate
             (send-sms-and-return-status!)
             (update-status-in-db! herate))))
    (log/info "Käsiteltiin" (count lahetettavat) "lähetettävää SMS-viestiä.")))
