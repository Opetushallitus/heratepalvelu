(ns oph.heratepalvelu.amis.EmailStatusHandler
  "Hakee viestintäpalvelussa olevian sähköpostien tilat viestintäpalvelusta ja
  ja päivittää ne tietokantaan."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.amis.AMISCommon :as ac]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.log.caller-log :refer :all])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.amis.EmailStatusHandler"
  :methods [[^:static handleEmailStatus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn update-ehoks-if-not-muistutus!
  "Päivittää sähköpostitiedot ehoksiin lähetyksen jälkeen, jos viesti ei ole
  muistutus."
  [herate status tila]
  ;; Täytyy hakea kokonainen heräte tietokannasta, koska argumenttina annettu
  ;; heräte on saatu indexin kautta, joka ei sisällä kaikkia kenttiä.
  (let [full-herate (ddb/get-item {:toimija_oppija [:s (:toimija_oppija herate)]
                                   :tyyppi_kausi [:s (:tyyppi_kausi herate)]})]
    (when-not (contains? #{1 2} (:muistutukset full-herate))
      (c/send-lahetys-data-to-ehoks
        (:toimija_oppija herate)
        (:tyyppi_kausi herate)
        {:kyselylinkki (:kyselylinkki herate)
         :lahetyspvm (first (str/split (:sendingEnded status) #"T"))
         :sahkoposti (:sahkoposti herate)
         :lahetystila tila}))))

(defn update-db-tila!
  "Päivittää sähköpostin tilan tietokantaan."
  [herate tila]
  (try
    (ac/update-herate herate {:lahetystila [:s tila]})
    (catch AwsServiceException e
      (log/error "Lähetystilan tallennus kantaan epäonnistui" herate)
      (log/error e))))

(defn do-query!
  "Hakee viestintäpalvelussa olevien herätteiden tiedot tietokannasta."
  []
  (ddb/query-items {:lahetystila
                    [:eq [:s (:viestintapalvelussa c/kasittelytilat)]]}
                   {:index "lahetysIndex"}))

(defn handle-single-herate!
  "Hakee yhden viestin tilan viestintäpalvelusta ja päivittää sen tietokantaan.
  Palauttaa, päivitettiinkö viestin tila."
  [herate]
  (log/info "Kysytään herätteen" herate "viestintäpalvelun tila")
  (let [status (vp/get-email-status (:viestintapalvelu-id herate))
        tila (vp/viestintapalvelu-status->kasittelytila status)]
    (if tila
      (try
        (log/info "Herätteellä on status" status "eli tila" tila)
        (update-db-tila! herate tila)
        (arvo/patch-kyselylinkki-metadata (:kyselylinkki herate) tila)
        (update-ehoks-if-not-muistutus! herate status tila)
        (catch Exception e
          (log/error e "Lähetystilan tallennus Arvoon/eHOKSiin epäonnistui"
                     herate)))
      (log/info "Heräte odottaa lähetystä:" status))
    tila))

(defn -handleEmailStatus
  "Päivittää viestintäpalvelussa olevien sähköpostien tilat tietokantaan."
  [_ event ^com.amazonaws.services.lambda.runtime.Context context]
  (log-caller-details-scheduled "handleEmailStatus" event context)
  (let [heratteet (do-query!)
        changed? (->> heratteet
                      (map handle-single-herate!)  ; avoid short circuit here
                      (reduce #(or %2 %1) false))]
    (log/info (if changed? "Handled" "Nothing to update"))))
