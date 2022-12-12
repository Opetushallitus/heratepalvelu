(ns oph.heratepalvelu.amis.AMISehoksTimedOperationsHandler
  "Käsittelee ajastettuja operaatioita AMIS-puolella."
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
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
  "Pyytää ehoksia lähettämään käsittelemättömät herätteet uudestaan ja
   käynnistää opiskelijan yhteystietojen poiston."
  [_ _ _]
  (log/info "Käynnistetään herätteiden lähetys")
  (let [resp (ehoks/get-retry-kyselylinkit "2021-07-01"
                                           (str (c/local-date-now))
                                           1000)]
    (log/info "Lähetetty" (:data (:body resp)) "viestiä"))

  (log/info "Käynnistetään opiskelijan yhteystietojen poisto")
  (let [hoksit (get-in (ehoks/delete-opiskelijan-yhteystiedot)
                       [:body :data :hoks-ids])]
    (doseq [hoks-id hoksit]
      (log/info "Poistetaan opiskelijan yhteystiedot (ehoks-id = "
                (:tjk-id hoks-id)
                ")")
      (ddb/update-item
       {:ehoks-id [:n hoks-id]}
       {:update-expr "SET #eml = :eml_value, #puh = :puh_value"
        :expr-attr-names {"#eml" "sahkoposti"
                          "#puh" "puhelinnumero"}
        :expr-attr-vals {":eml_value" [:s nil] ":puh_value" [:s nil]}}
       (:herate-table env)))
    (log/info "Poistettu" (count hoksit) "opiskelijan yhteystiedot")))

(defn -handleMassHerateResend
  "Pyytää ehoksia lähettämäan viime 2 viikon herätteet uudestaan."
  [_ _ _]
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
  [_ _ _]
  (log/info "Käynnistetään ehoksin opiskeluoikeuksien päivitys")
  (ehoks/update-ehoks-opiskeluoikeudet))
