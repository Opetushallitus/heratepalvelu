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

(defn update-ehoks-if-not-muistutus
  "Päivittää sähköpostitiedot ehoksiin lähetyksen jälkeen, jos viesti ei ole
  muistutus."
  [herate status tila]
  ;; Täytyy hakea kokonainen heräte tietokannasta, koska argumenttina annettu
  ;; heräte on saatu indexin kautta, joka ei sisällä kaikkia kenttiä.
  (let [full-herate (ddb/get-item {:toimija_oppija [:s (:toimija_oppija herate)]
                                   :tyyppi_kausi [:s (:tyyppi_kausi herate)]})]
    (when-not (.contains [1 2] (:muistutukset full-herate))
      (c/send-lahetys-data-to-ehoks
        (:toimija_oppija herate)
        (:tyyppi_kausi herate)
        {:kyselylinkki (:kyselylinkki herate)
         :lahetyspvm (first (str/split (:sendingEnded status) #"T"))
         :sahkoposti (:sahkoposti herate)
         :lahetystila tila}))))

(defn update-db
  "Päivittää sähköpostin tilan tietokantaan."
  [herate tila]
  (try
    (ddb/update-item
      {:toimija_oppija [:s (:toimija_oppija herate)]
       :tyyppi_kausi   [:s (:tyyppi_kausi herate)]}
      {:update-expr    "SET #lahetystila = :lahetystila"
       :expr-attr-names {"#lahetystila" "lahetystila"}
       :expr-attr-vals  {":lahetystila" [:s tila]}})
    (catch AwsServiceException e
      (log/error "Lähetystilan tallennus kantaan epäonnistui" herate)
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
  (loop [heratteet (do-query)]
    (doseq [herate heratteet]
      (let [status (vp/get-email-status (:viestintapalvelu-id herate))
            tila (vp/convert-email-status status)]
        (if tila
          (do
            (when (not @new-changes?)
              (reset! new-changes? true))
            (try
              (arvo/patch-kyselylinkki-metadata (:kyselylinkki herate) tila)
              (update-ehoks-if-not-muistutus herate status tila)
              (update-db herate tila)
              (catch Exception e
                (log/error "Lähetystilan tallennus Arvoon epäonnistui" herate)
                (log/error e))))
          (do
            (log/info "Odottaa lähetystä viestintäpalvelussa")
            (log/info herate)
            (log/info status)))))
    (when (and @new-changes?
               (< 120000 (.getRemainingTimeInMillis context)))
      (reset! new-changes? false)
      (recur (do-query)))))
