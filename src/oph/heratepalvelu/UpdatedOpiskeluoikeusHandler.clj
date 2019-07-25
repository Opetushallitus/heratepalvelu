(ns oph.heratepalvelu.UpdatedOpiskeluoikeusHandler
  (:require
    [oph.heratepalvelu.external.koski :refer [get-updated-opiskeluoikeudet]]
    [oph.heratepalvelu.external.ehoks :refer [get-hoks-by-opiskeluoikeus]]
    [oph.heratepalvelu.db.dynamodb :as ddb]
    [oph.heratepalvelu.common :refer :all]
    [environ.core :refer [env]]))

(gen-class
  :name "oph.heratepalvelu.UpdatedOpiskeluoikeusHandler"
  :methods [[^:static handleUpdatedOpiskeluoikeus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn -handleUpdatedOpiskeluoikeus [this event context]
  (let [last-modified (:timestamp (ddb/get-item
                                    {:type [:s "opiskeluoikeus"]}
                                    (:checkedlast-table env)))]
    (loop [opiskeluoikeudet (sort-by :updated-at
                                     (get-updated-opiskeluoikeudet
                                       last-modified))]
      (when (seq opiskeluoikeudet)
        (doseq [opiskeluoikeus opiskeluoikeudet]
          (let [koulustoimija (get-koulutustoimija-oid opiskeluoikeus)
                suoritus (first (seq (:suoritukset opiskeluoikeus)))
                vahvistus-pvm (get-vahvistus-pvm opiskeluoikeus)]
            (when (and (some? vahvistus-pvm)
                       (check-suoritus-type? suoritus)
                       (check-organisaatio-whitelist?
                         koulustoimija (:updated-at opiskeluoikeus)))
              (let [hoks (get-hoks-by-opiskeluoikeus (:oid opiskeluoikeus))]
                (cond
                  (= (:tyyppi suoritus)
                     "ammatillinentutkinto")
                  (save-herate (assoc hoks :ehoks-id (:id hoks))
                               opiskeluoikeus
                               "tutkinnon_suorittaneet"
                               vahvistus-pvm)
                  (= (:tyyppi suoritus)
                     "ammatillinentutkintoosittainen")
                  (save-herate (assoc hoks :ehoks-id (:id hoks))
                               opiskeluoikeus
                               "tutkinnon_osia_suorittaneet"
                               vahvistus-pvm))))))
        (ddb/update-item
          {:type [:s "opiskeluoikeus"]}
          {:update-expr     "SET #timestamp = :timestamp"
           :expr-attr-names {"#timestamp" "timestamp"}
           :expr-attr-vals {":timestamp"
                            [:n (str (:updated-at (last opiskeluoikeudet)))]}}
          (:checkedlast-table env))
        (when (> 30000 (.getRemainingTimeInMillis context))
          (recur (sort-by :updated-at
                          (get-updated-opiskeluoikeudet
                            (:updated-at (last opiskeluoikeudet))))))))))