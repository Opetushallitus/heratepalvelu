(ns oph.heratepalvelu.tep.kestojenUudelleenlaskentaHandler
  "Handler työpaikkajaksojen kestojen uudelleenlaskentaa varten."
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.log.caller-log :refer
             [log-caller-details-scheduled]]
            [oph.heratepalvelu.tep.niputusHandler :as nh]
            [oph.heratepalvelu.tep.tepCommon :as tc]))

(gen-class :name "oph.heratepalvelu.tep.kestojenUudelleenlaskentaHandler"
           :methods
           [[^:static handleKestojenUudelleenlaskenta
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(def alkupvm "2023-07-01")
(def koulutustoimija-oid "1.2.246.562.10.82246911869")

(defn scan-for-jaksot-with-kesto!
  "Hakee tietokannasta koulutustoimijan jaksot, jotka ovat päättyneet
  `alkupvm` jälkeen ja joille on laskettu niputuksen yhteydessä kesto. Näille
  jaksoille on jo kerran suoritettu uudelleenlaskenta, joten `kesto_vanha`
  tiedon sijasta hyödynnetään `kestojen_uudelleenlaskentakerta` attribuuttia,
  jotta suoritus etenisi eikä `scan` palauttaisi samoja jaksoja uudelleen ja
  uudelleen aina seuraavaan `scan`-operaation yhteydessä."
  [exclusive-start-key]
  (ddb/scan {:filter-expression
             (str "koulutustoimija = :koulutustoimija AND
                  jakso_loppupvm >= :alkupvm AND "
                  "attribute_exists(kesto) AND "
                  "kestojen_uudelleenlaskentakerta = :kerta")
             :exclusive-start-key exclusive-start-key
             :expr-attr-vals      {":koulutustoimija" [:s koulutustoimija-oid]
                                   ":alkupvm"         [:s alkupvm]
                                   ":kerta"           [:n 2]}}
            (:jaksotunnus-table env)))

(defn- get-old-kesto
  "If `kesto_vanha` field already exists in `jakso`, return that, otherwise
  return `:kesto`."
  [jakso]
  (or (:kesto_vanha jakso) (:kesto jakso)))

(defn -handleKestojenUudelleenlaskenta
  [_ event ^com.amazonaws.services.lambda.runtime.Context context]
  (log-caller-details-scheduled "handleKestojenUudelleenlaskenta" event context)
  (loop [resp (scan-for-jaksot-with-kesto! nil)]
    (log/info "Processing" (count (:items resp)) "jaksoa.")
    (doseq [jakso (:items resp)]
      (let [concurrent-jaksot (nh/get-concurrent-jaksot-from-ehoks! [jakso])
            opiskeluoikeudet  (nh/get-and-memoize-opiskeluoikeudet!
                                concurrent-jaksot)
            jakso-key         (nh/ids jakso)]
        (cond
          (not-any? #(= (nh/ids %) jakso-key) concurrent-jaksot)
          (do (log/warnf (str "Couldn't calculate duriation for jakso %s "
                              "because it cannot be found from eHOKS. Setting "
                              "newly calculated kesto to 0.")
                         jakso-key)
              (tc/update-jakso jakso
                               {:kesto [:n 0]
                                :kesto_vanha [:n (get-old-kesto jakso)]
                                :nollakeston_syy [:s "Jakso poistettu"]
                                :kestojen_uudelleenlaskentakerta [:n 3]}))

          (not (contains? opiskeluoikeudet (:opiskeluoikeus_oid jakso)))
          (do (log/warnf (str "Couldn't calculate duriation for jakso %s "
                              "because couldn't get opiskeluoikeus from Koski. "
                              "Setting newly calculated kesto to 0.")
                         jakso-key)
              (tc/update-jakso
                jakso
                {:kesto [:n 0]
                 :kesto_vanha [:n (get-old-kesto jakso)]
                 :nollakeston_syy [:s "Ei saatu opiskeluoikeutta Koskesta"]
                 :kestojen_uudelleenlaskentakerta [:n 3]}))

          :else
          (if-let [new-kesto (get (nh/oppijan-jaksojen-kestot
                                    concurrent-jaksot opiskeluoikeudet)
                                  jakso-key)]
            (do (log/info "Updating jakso" jakso-key "with kesto" new-kesto)
                (tc/update-jakso jakso
                                 {:kesto [:n new-kesto]
                                  :kesto_vanha [:n (get-old-kesto jakso)]
                                  :kestojen_uudelleenlaskentakerta [:n 3]}))
            (do (log/errorf (str "For unknown reason, couldn't calculate kesto "
                                 "for jakso with ids %s. Setting newly "
                                 "calculated kesto to 0.")
                            jakso-key)
                (tc/update-jakso jakso
                                 {:kesto [:n 0]
                                  :kesto_vanha [:n (get-old-kesto jakso)]
                                  :nollakeston_syy [:s "Tuntematon"]
                                  :kestojen_uudelleenlaskentakerta [:n 3]}))))))
    (when (and (< 30000 (.getRemainingTimeInMillis context))
               (:last-evaluated-key resp))
      (recur (scan-for-jaksot-with-kesto! (:last-evaluated-key resp))))))
