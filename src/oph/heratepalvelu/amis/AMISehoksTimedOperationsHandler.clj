(ns oph.heratepalvelu.amis.AMISehoksTimedOperationsHandler
  "Käsittelee ajastettuja operaatioita AMIS-puolella."
  (:require [clojure.tools.logging :as log]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.ehoks :as ehoks]))

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

(defn -handleAMISTimedOperations
  "Pyytää ehoksia lähettämään käsittelemättömät herätteet uudestaan."
  [this event context]
  (log/info "Käynnistetään herätteiden lähetys")
  (let [resp (ehoks/get-retry-kyselylinkit "2021-07-01"
                                           (str (c/local-date-now))
                                           1000)]
    (log/info "Lähetetty" (:data (:body resp)) "viestiä")))

(defn -handleMassHerateResend
  "Pyytää ehoksia lähettämäan viime 2 viikon herätteet uudestaan."
  [this event context]
  (log/info "Käynnistetään herätteiden massauudelleenlähetys")
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
  [this event context]
  (log/info "Käynnistetään ehoksin opiskeluoikeuksien päivitys")
  (ehoks/update-ehoks-opiskeluoikeudet))
