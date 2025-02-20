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
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)
           (java.time LocalDate)))

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
  "Päivittää sähköpostin ja uuden voimassaolon loppupäivän tilan tietokantaan."
  [herate tila new-alkupvm new-loppupvm]
  (try
    (ac/update-herate herate (cond-> {:lahetystila [:s tila]}
                               (and new-alkupvm new-loppupvm)
                               (assoc :alkupvm [:s new-alkupvm]
                                      :voimassa-loppupvm [:s new-loppupvm])))
    (catch AwsServiceException e
      (log/error "Lähetystilan tallennus kantaan epäonnistui" herate)
      (log/error e))))

(defn do-query!
  "Hakee viestintäpalvelussa olevien herätteiden tiedot tietokannasta."
  []
  (ddb/query-items {:lahetystila
                    [:eq [:s (:viestintapalvelussa c/kasittelytilat)]]}
                   {:index "lahetysIndex"}))

; TODO enable/remove feature-flag after API change is implemented in Arvo
(def use-new-endpoint-for-vastauslinkki-patch? false)

(defn get-new-loppupvm
  [herate]
  (when (and
          use-new-endpoint-for-vastauslinkki-patch?
          (not (or (= (:lahetystila herate) (:success c/kasittelytilat))
                   (= (:lahetystila herate) (:vastattu c/kasittelytilat))))
          (not= (LocalDate/parse (:alkupvm herate)) (c/local-date-now)))
    (str (c/loppu (LocalDate/parse (:heratepvm herate))
                  (c/local-date-now)))))

(defn handle-single-herate!
  "Hakee yhden viestin tilan viestintäpalvelusta ja päivittää sen tietokantaan.
  Palauttaa, päivitettiinkö viestin tila."
  [herate]
  (log/info "Kysytään herätteen" herate "viestintäpalvelun tila")
  (try
    (let [status (vp/get-email-status (:viestintapalvelu-id herate))
          tila (vp/viestintapalvelu-status->kasittelytila status)
          new-alkupvm (when (and use-new-endpoint-for-vastauslinkki-patch?
                                 (= tila (:success c/kasittelytilat)))
                        (str (c/local-date-now)))
          new-loppupvm (when (and use-new-endpoint-for-vastauslinkki-patch?
                                  (= tila (:success c/kasittelytilat)))
                         (get-new-loppupvm herate))]
      (if tila
        (do
          (log/info "Herätteellä on status" status "eli tila" tila)
          (update-db-tila! herate tila new-alkupvm new-loppupvm)
          (if use-new-endpoint-for-vastauslinkki-patch?
            (arvo/patch-kyselylinkki
              (:kyselylinkki herate) tila new-alkupvm new-loppupvm)
            (arvo/patch-kyselylinkki-metadata
              (:kyselylinkki herate) tila))
          (update-ehoks-if-not-muistutus! herate status tila))
        (log/info "Heräte odottaa lähetystä:" status))
      tila)
    (catch Exception e (log/error e "herätteellä" herate))))

(defn -handleEmailStatus
  "Päivittää viestintäpalvelussa olevien sähköpostien tilat tietokantaan."
  [_ event ^com.amazonaws.services.lambda.runtime.Context context]
  (log-caller-details-scheduled "handleEmailStatus" event context)
  (let [heratteet (do-query!)
        timeout? (c/no-time-left? context 60000)]
    (log/info "Aiotaan käsitellä" (count heratteet) "herätettä")
    (c/doseq-with-timeout
      timeout?
      [herate heratteet]
      (handle-single-herate! herate))))
