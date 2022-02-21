(ns oph.heratepalvelu.amis.EmailStatusHandler
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.external.arvo :as arvo]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.common :as c]
            [clojure.string :as str])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))

;; Hakee viestintäpalvelussa olevian sähköpostien tilat viestintäpalvelusta ja
;; ja päivittää ne tietokantaan.

(gen-class
  :name "oph.heratepalvelu.amis.EmailStatusHandler"
  :methods [[^:static handleEmailStatus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])
(def ^:private new-changes? (atom false))

(defn convert-vp-email-status
  "Muuttaa viestintäpalvelusta palautetun statuksen käsittelytilaksi."
  [status]
  (if (= (:numberOfSuccessfulSendings status) 1)
    (:success c/kasittelytilat)
    (if (= (:numberOfBouncedSendings status) 1)
      (:bounced c/kasittelytilat)
      (when (= (:numberOfFailedSendings status) 1)
        (:failed c/kasittelytilat)))))

(defn update-ehoks-if-not-muistutus
  "Päivittää sähköpostitiedot ehoksiin lähetyksen jälkeen, jos viesti ei ole
  muistutus."
  [email status tila]
  (let [full-email (ddb/get-item {:toimija_oppija [:s (:toimija_oppija email)]
                                  :tyyppi_kausi [:s (:tyyppi_kausi email)]})]
    (when-not (.contains [1 2] (:muistutukset full-email))
      (c/send-lahetys-data-to-ehoks
        (:toimija_oppija email)
        (:tyyppi_kausi email)
        {:kyselylinkki (:kyselylinkki email)
         :lahetyspvm (first (str/split (:sendingEnded status) #"T"))
         :sahkoposti (:sahkoposti email)
         :lahetystila tila}))))

(defn update-db
  "Päivittää sähköpostin tiedot ja tilan tietokantaan."
  [email tila]
  (try
    (ddb/update-item
      {:toimija_oppija [:s (:toimija_oppija email)]
       :tyyppi_kausi   [:s (:tyyppi_kausi email)]}
      {:update-expr    "SET #lahetystila = :lahetystila"
       :expr-attr-names {"#lahetystila" "lahetystila"}
       :expr-attr-vals  {":lahetystila" [:s tila]}})
    (catch AwsServiceException e
      (log/error "Lähetystilan tallennus kantaan epäonnistui" email)
      (log/error e))))

(defn do-query
  "Hakee viestintäpalvelussa olevien herätteiden tiedot tietokannasta."
  []
  (ddb/query-items {:lahetystila [:eq
                                  [:s
                                   (:viestintapalvelussa c/kasittelytilat)]]}
                   {:index "lahetysIndex"
                    :limit 10}))

(defn -handleEmailStatus
  "Päivittää viestintäpalvelussa olevien sähköpostien tilat tietokantaan."
  [this event context]
  (log-caller-details-scheduled "handleEmailStatus" event context)
  (loop [emails (do-query)]
    (doseq [email emails]
      (let [status (vp/get-email-status (:viestintapalvelu-id email))
            tila (convert-vp-email-status status)]
        (if tila
          (do
            (when (not @new-changes?)
              (reset! new-changes? true))
            (try
              (arvo/patch-kyselylinkki-metadata (:kyselylinkki email) tila)
              (update-ehoks-if-not-muistutus email status tila)
              (update-db email tila)
              (catch Exception e
                (log/error "Lähetystilan tallennus Arvoon epäonnistui" email)
                (log/error e))))
          (do
            (log/info "Odottaa lähetystä viestintäpalvelussa")
            (log/info email)
            (log/info status)))))
    (when (and @new-changes?
               (< 120000 (.getRemainingTimeInMillis context)))
      (reset! new-changes? false)
      (recur (do-query)))))
