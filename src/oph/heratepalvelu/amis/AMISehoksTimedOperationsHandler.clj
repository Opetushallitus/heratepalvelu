(ns oph.heratepalvelu.amis.AMISehoksTimedOperationsHandler
  "Käsittelee ajastettuja operaatioita AMIS-puolella."
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.log.caller-log :refer [log-caller-details-sqs]]
            [oph.heratepalvelu.external.ehoks :as ehoks])
  (:import (com.amazonaws.services.lambda.runtime.events SQSEvent)))

(gen-class
  :name "oph.heratepalvelu.amis.AMISehoksTimedOperationsHandler"
  :methods [[^:static handleAMISTimedOperations
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]
            [^:static handleMassHerateResend
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]
            [^:static handleEhoksOpiskeluoikeusUpdate
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn ehoks-request-missing-kyselylinkit!
  "Pyytää ehoksia lähettämään käsittelemättömät herätteet uudestaan"
  []
  (log/info "Pyydetään eHOKSia lähettämään uudestaan herätteet kaikille"
            "aloittaneille tai suorituksia saaneille,"
            "joilla ei ole vielä kyselylinkkiä")
  (let [resp (ehoks/send-kasittelemattomat-heratteet!
               (str (c/current-rahoituskausi-alkupvm))
               (str (c/local-date-now))
               1000)]
    (log/info "Lähetetty" (:data (:body resp)) "viestiä")))

(defn delete-oppija-contact!
  "Poistaa yhden oppijat yhteystiedot tietokannasta HOKS-id:n perusteella."
  [hoks-id]
  (log/info "Hoks ID" hoks-id)
  (let [resp (ddb/scan {:filter-expression   "#id = :id"
                        :expr-attr-names     {"#id" "ehoks-id"}
                        :expr-attr-vals      {":id" [:n hoks-id]}}
                       (:herate-table env))
        items (:items resp)]
    (doseq [item items]
      (log/info "Poistetaan opiskelijan yhteystiedot; toimija_oppija ="
                (:toimija_oppija item) ", tyyppi_kausi =" (:tyyppi_kausi item))
      (ddb/update-item
        {:toimija_oppija [:s (:toimija_oppija item)]
         :tyyppi_kausi   [:s (:tyyppi_kausi item)]}
        {:update-expr     "SET #eml = :eml_value, #puh = :puh_value"
         :expr-attr-names {"#eml" "sahkoposti"
                           "#puh" "puhelinnumero"}
         :expr-attr-vals  {":eml_value" [:s ""] ":puh_value" [:s ""]}}
        (:herate-table env)))))

(defn delete-oppija-contacts-from-old-hoksit-and-heratteet!
  "Pyytää ehoksia poistamaan opiskelijoiden yhteystiedot vanhoista HOKSeista
  ja poistaa herätepalvelun tiedot samoista HOKSeista."
  []
  (log/info "Käynnistetään opiskelijan yhteystietojen poisto")
  (let [hoks-ids (get-in (ehoks/delete-opiskelijan-yhteystiedot)
                         [:body :data :hoks-ids])]
    (log/info "Käydään läpi" (count hoks-ids) "hoksin tiedot")
    (doseq [hoks-id hoks-ids]
      (delete-oppija-contact! hoks-id))))

(defn -handleAMISTimedOperations
  "Pyytää ehoksia lähettämään käsittelemättömät herätteet uudestaan ja
   käynnistää opiskelijan yhteystietojen poiston."
  [_ ^SQSEvent event context]
  (log-caller-details-sqs "handleAMISTimedOperations" context)
  (ehoks-request-missing-kyselylinkit!)
  (delete-oppija-contacts-from-old-hoksit-and-heratteet!))

(defn -handleMassHerateResend
  "Pyytää ehoksia lähettämäan viime 2 viikon herätteet uudestaan."
  [_ ^SQSEvent event context]
  (log-caller-details-sqs "handleMassHerateResend" context)
  (let [now          (c/local-date-now)
        start        (str (.minusDays now 14))
        end          (str now)
        aloitus-resp (ehoks/resend-aloitusheratteet start end)
        paatto-resp  (ehoks/resend-paattoheratteet start end)]
    (log/info "Lähetetty"
              (:count (:data (:body aloitus-resp)))
              "aloitusviestiä ja"
              (:count (:data (:body paatto-resp)))
              "päättöviestiä")))

(defn -handleEhoksOpiskeluoikeusUpdate
  "Pyytää ehoksia päivittämään opiskeluoikeuksien hankintakoulutukset."
  [_ ^SQSEvent event context]
  (log-caller-details-sqs "handleEhoksOpiskeluoikeusUpdate" context)
  (let [result (ehoks/update-ehoks-opiskeluoikeudet)]
    (log/info "update-ehoks-opiskeluoikeudet result:" result)))
