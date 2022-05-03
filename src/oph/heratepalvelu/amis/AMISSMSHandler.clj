(ns oph.heratepalvelu.amis.AMISSMSHandler
  "";; TODO
  (:require [clojure.tools.logging :as log]
            [oph.heratepalvelu.amis.AMISCommon :as ac]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.elisa :as elisa]
            [oph.heratepalvelu.log.caller-log :as cl])
  (:import (clojure.lang ExceptionInfo)
           (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.amis.AMISSMSHandler"
  :methods [[^:static handleAMISSMS
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn query-lahetettavat
  "" ;; TODO
  [limit]
  ;; TODO

  )

(defn -handleAMISSMS
  "" ;; TODO
  [_ event ^com.amazonaws.services.lambda.runtime.Context context]
  (cl/log-caller-details-scheduled "AMISSMSHandler" event context)
  (loop [lahetettavat (query-lahetettavat 20)]
    (log/info "Käsitellään" (count lahetettavat) "lähetettävää SMS-viestiä.")
    (when (seq lahetettavat)
      (doseq [herate lahetettavat]
        (if (c/has-time-to-answer? (:voimassa-loppupvm herate))
          (if (c/valid-number? (:puhelinnumero herate))
            (try
              (let [numero (:puhelinnumero herate)
                    body (elisa/amis-msg-body (:kyselylinkki herate))
                    resp (elisa/send-sms numero body)
                    results (get-in resp [:body :message (keyword numero)])]
                (ac/update-herate
                  herate ;; TODO onko meillä uusi loppupvm?
                  {:sms-kasittelytila [:s (:status results)]
                   :sms-lahetyspvm    [:s (str (c/local-date-now))]
                   :lahetettynumeroon [:s (or (:converted results) numero)]})

              ;; TODO arvo patch?
              ;; TODO patch in ehoks?

              )
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
                   {:sms-lahetyspvm    [:s (str (c/local-date-now))]
                    :sms-kasittelytila [:s (:phone-invalid c/kasittelytilat)]})
              (catch Exception e
                (log/error "Virhe AMIS SMS-lähetystila päivistykessä kun"
                           "puhelinnumero ei ollut validi."
                           e))))
          (try
            (ac/update-herate herate
                              {:sms-lahetyspvm [:s (str (c/local-date-now))]
                               :sms-kasittelytila
                               [:s (:vastausaika-loppunut c/kasittelytilat)]})
            (catch Exception e
              (log/error "Virhe AMIS SMS-lähetystilan päivityksessä kun"
                         "vastausaika on umpeutunut:"
                         e)))))
      (when (< 60000 (.getRemainingTimeInMillis context))
        (recur (query-lahetettavat 20))))))
