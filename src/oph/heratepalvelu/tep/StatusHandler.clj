(ns oph.heratepalvelu.tep.StatusHandler
  "Käsittelee viestintäpalvelussa olevien viestien tiloja."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.tep.tepCommon :as tc])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.tep.StatusHandler"
  :methods [[^:static handleEmailStatus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(def ^:private new-changes?
  "Atom, jolla pidetään kiinni siitä, onko uusia muutoksia tapahtunut."
  (atom false))

(defn -handleEmailStatus
  "Hakee nippuja, joilla on sähköpostiviestejä viestintäpalvelussa, ja päivittää
  niiden tiedot tietokantaan ja Arvoon. Laskee uuden loppupäivämäärän nipulle,
  jos kyselyyn ei ole vielä vastattu ja kyselyä ei ole vielä lähetetty."
  [_ event ^com.amazonaws.services.lambda.runtime.Context context]
  (log-caller-details-scheduled "handleEmailStatus" event context)
  (let [emails (ddb/query-items
                 {:kasittelytila
                  [:eq [:s (:viestintapalvelussa c/kasittelytilat)]]}
                 {:index "niputusIndex"}
                 (:nippu-table env))
        timeout? (c/no-time-left? context 60000)]
    (log/info "Aiotaan käsitellä" (count emails) "sähköpostiviestiä.")
    (c/doseq-with-timeout
      timeout?
      [email emails]
      (log/info "Päivitetään tila viestintäpalvelussa olevalle nipulle" email)
      (try
        (let [nippu (ddb/get-item {:ohjaaja_ytunnus_kj_tutkinto
                                   [:s (:ohjaaja_ytunnus_kj_tutkinto email)]
                                   :niputuspvm [:s (:niputuspvm email)]}
                                  (:nippu-table env))
              status (vp/get-email-status (:viestintapalvelu-id nippu))
              tila (vp/viestintapalvelu-status->kasittelytila status)
              new-loppupvm (tc/get-new-loppupvm nippu)]
          (if-not tila
            (log/info "Odottaa lähetystä viestintäpalvelussa:" status)
            (do
              (when-not @new-changes? (reset! new-changes? true))
              (log/info "Päivitetään Arvoon tila" tila "loppupvm" new-loppupvm)
              (if (or (str/includes? (:kyselylinkki nippu) ",")
                      (str/includes? (:kyselylinkki nippu) ";"))
                (log/warn "Kyselylinkissä outoja merkkejä, ei päivitetä Arvoon:"
                          (:kyselylinkki nippu))
                (arvo/patch-nippulinkki
                  (:kyselylinkki nippu)
                  (if (and new-loppupvm (= tila (:success c/kasittelytilat)))
                    {:tila tila :voimassa_loppupvm new-loppupvm}
                    {:tila tila :voimassa_loppupvm (:voimassaloppupvm nippu)})))
              (log/info "Päivitetään tietokantaan tila" tila
                        "loppupvm" new-loppupvm)
              (tc/update-nippu
                nippu
                {:kasittelytila [:s tila]
                 :voimassaloppupvm [:s (or new-loppupvm
                                           (:voimassaloppupvm nippu))]}))))
        (catch Exception e
          (log/error e "mailille" email))))))
