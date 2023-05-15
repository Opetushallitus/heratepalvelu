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
  [limit]
  (ddb/query-items-with-expression
    "#smstila = :tila AND #alku <= :pvm"
    {:index "smsIndex"
     :filter-expression "attribute_exists(#linkki)"
     :expr-attr-names {"#smstila" "sms-lahetystila"
                       "#alku" "alkupvm"
                       "#linkki" "kyselylinkki"}
     :expr-attr-vals {":tila" [:s (:ei-lahetetty c/kasittelytilat)]
                      ":pvm" [:s (str (c/local-date-now))]}
     :limit limit}
    (:herate-table env)))

(defn -handleAMISSMS
  "Hakee tietokannasta herätteitä, joilta SMS-viesti ei ole vielä lähetetty, ja
  käsittelee viestien lähetystä."
  [_ event ^com.amazonaws.services.lambda.runtime.Context context]
  (cl/log-caller-details-scheduled "AMISSMSHandler" event context)
  (loop [lahetettavat (query-lahetettavat 20)]
    (log/info "Käsitellään" (count lahetettavat) "lähetettävää SMS-viestiä.")
    (when (seq lahetettavat)
      (doseq [herate lahetettavat]
        (if (c/has-time-to-answer? (:voimassa-loppupvm herate))
          (if (and
                (some? (:puhelinnumero herate))
                (c/valid-number? (:puhelinnumero herate)))
            (try
              (let [numero (:puhelinnumero herate)
                    oppilaitos (org/get-organisaatio (:oppilaitos herate))
                    oppilaitos-nimi (:fi (:nimi oppilaitos))
                    body (elisa/amis-msg-body (:kyselylinkki herate)
                                              oppilaitos-nimi)
                    resp (elisa/send-sms numero body)
                    results (get-in resp [:body :messages (keyword numero)])]
                (ac/update-herate
                  herate ;; TODO uuden alkupvm asettaminen
                  {:sms-lahetystila   [:s (:status results)]
                   :sms-lahetyspvm    [:s (str (c/local-date-now))]
                   :lahetettynumeroon [:s (or (:converted results) numero)]}))
              (catch AwsServiceException e
                (log/error "AMIS SMS-viestin lähetysvaiheen kantapäivityksessä"
                           "tapahtui virhe!"
                           e))
              (catch ExceptionInfo e
                (let [error-type (if (c/client-error? e) "Client" "Server")]
                  (log/error error-type "error while sending AMIS SMS" e)))
              (catch Exception e
                (log/error "Virhe AMIS SMS-lähetyksessä:" e)))
            (try (ac/update-herate
                   herate
                   {:sms-lahetyspvm  [:s (str (c/local-date-now))]
                    :sms-lahetystila [:s (:phone-invalid c/kasittelytilat)]})
                 (catch Exception e
                   (log/error "Virhe AMIS SMS-lähetystila päivistykessä kun"
                              "puhelinnumero ei ollut validi."
                              e))))
          (try
            (ac/update-herate
              herate
              {:sms-lahetyspvm [:s (str (c/local-date-now))]
               :sms-lahetystila [:s (:vastausaika-loppunut c/kasittelytilat)]})
            (catch Exception e
              (log/error "Virhe AMIS SMS-lähetystilan päivityksessä kun"
                         "vastausaika on umpeutunut:"
                         e)))))
      (when (< 60000 (.getRemainingTimeInMillis context))
        (recur (query-lahetettavat 20))))))
