(ns oph.heratepalvelu.tep.tmpKestoCalculationHandler
  "A temporary handler for calculating missing kestot (see EH-1900)"
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.log.caller-log :refer
             [log-caller-details-scheduled]]
            [oph.heratepalvelu.tep.niputusHandler :as nh]
            [oph.heratepalvelu.tep.tepCommon :as tc]))

(gen-class :name "oph.heratepalvelu.tep.tmpKestoCalculationHandler"
           :methods
           [[^:static calculateMissingKestot
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(def rahoituskausi {:start-date "2024-07-01"
                    :end-date   "2025-06-30"})

(defn scan-for-jaksot-without-kesto!
  "Scans for jaksot in `jaksoTunnusTable`
  Hakee tietokannasta koulutustoimijan jaksot, jotka ovat päättyneet
  `alkupvm` jälkeen ja joille on laskettu niputuksen yhteydessä kesto. Näille
  jaksoille on jo kerran suoritettu uudelleenlaskenta, joten `kesto_vanha`
  tiedon sijasta hyödynnetään `kestojen_uudelleenlaskentakerta` attribuuttia,
  jotta suoritus etenisi eikä `scan` palauttaisi samoja jaksoja uudelleen ja
  uudelleen aina seuraavaan `scan`-operaation yhteydessä."
  [exclusive-start-key]
  (ddb/scan {:filter-expression
             (str "jakso_loppupvm >= :alkupvm AND "
                  "jakso_loppupvm <= :loppupvm AND "
                  "attribute_not_exists(kesto) AND "
                  "attribute_not_exists(mitatoity)")
             :exclusive-start-key exclusive-start-key
             :expr-attr-vals {":alkupvm"  [:s (:start-date rahoituskausi)]
                              ":loppupvm" [:s (:end-date rahoituskausi)]}}
            (:jaksotunnus-table env)))

(defn- update-jakso-with-kesto!
  [jakso kesto nollakeston-syy]
  (let [changes {:kesto                  [:n kesto]
                 :eh_1900_kestonlaskenta [:bool true]}]
    (tc/update-jakso jakso
                     (if nollakeston-syy
                       (assoc changes :nollakeston_syy [:s nollakeston-syy])
                       changes))))

(defn -calculateMissingKestot
  [_ event ^com.amazonaws.services.lambda.runtime.Context context]
  (log-caller-details-scheduled "calculateMissingKestot" event context)
  (loop [resp (scan-for-jaksot-without-kesto! nil)]
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
                              "calculated kesto to 0.")
                         jakso-key)
              (update-jakso-with-kesto! jakso 0 "Jakso poistettu"))

          (not (contains? opiskeluoikeudet (:opiskeluoikeus_oid jakso)))
          (do (log/warnf (str "Couldn't calculate duriation for jakso %s "
                              "because couldn't get opiskeluoikeus from Koski. "
                              "Setting newly calculated kesto to 0.")
                         jakso-key)
              (update-jakso-with-kesto!
                jakso 0 "Ei saatu opiskeluoikeutta Koskesta"))

          :else
          (if-let [kesto (get (nh/oppijan-jaksojen-kestot
                                concurrent-jaksot opiskeluoikeudet)
                              jakso-key)]
            (do (log/info "Updating jakso" jakso-key "with kesto" kesto)
                (update-jakso-with-kesto! jakso kesto nil))
            (do (log/errorf (str "For unknown reason, couldn't calculate kesto "
                                 "for jakso with ids %s. Setting newly "
                                 "calculated kesto to 0.")
                            jakso-key)
                (update-jakso-with-kesto! jakso 0 "Tuntematon"))))))
    (when (and (< 30000 (.getRemainingTimeInMillis context))
               (:last-evaluated-key resp))
      (recur (scan-for-jaksot-without-kesto! (:last-evaluated-key resp))))))
