(ns oph.heratepalvelu.amis.AMISherateEmailHandler
  "Lähettää herätteiden sähköpostiviestit viestintäpalveluun."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.amis.AMISCommon :as ac]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.external.ehoks :as ehoks]
            [oph.heratepalvelu.external.koski :as k]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.log.caller-log :refer :all])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)
           (java.time LocalDate)))

(gen-class
  :name "oph.heratepalvelu.amis.AMISherateEmailHandler"
  :methods [[^:static handleSendAMISEmails
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn update-kyselytunnus-in-ehoks!
  "Vie eHOKSiin tiedon uudesta kyselytunnuksesta."
  [herate]
  (let [req {:kyselylinkki (:kyselylinkki herate)
             :tyyppi       (:kyselytyyppi herate)
             :alkupvm      (:alkupvm herate)
             :lahetystila  (:ei-lahetetty c/kasittelytilat)}]
    (try (ehoks/add-kyselytunnus-to-hoks (c/hoks-id herate) req)
         (catch Exception e
           (log/error e "Virhe kyselylinkin lähetyksessä eHOKSiin"
                      "Request:" req
                      "Response:" (:body (ex-data e)))
           (throw e)))))

(defn update-and-return-herate!
  "Makes the given updates to both DDB and the argument herate
  (returned with the changes)"
  [herate changes]
  (ac/update-herate herate changes)
  (into herate (map #(vector %1 (second %2)) (keys changes) (vals changes))))

(defn- map-values
  "Helper function for mapping hashmap values."
  [m f]
  (zipmap (keys m) (map f (vals m))))

(defn- koodiarvo
  "Gets koodiarvo from koodi-uri. For example
  osaamisenhankkimistapa_oppisopimus -> oppisopimus"
  [koodi-uri]
  (last (str/split koodi-uri #"_")))

(defn- osaamisen-hankkimistapa-mapper
  "Returns a mapper function for osaamisen hankkimistapa. Mapper function
  produces a hashmap such as:
  {:hankkimistapa \"oppisopimus\" :tutkinnonosa \"234567\"}"
  [tutkinnon-osa-koodi-uri]
  (fn [osaamisen-hankkimistapa]
    {:hankkimistapa
     (koodiarvo (:osaamisen-hankkimistapa-koodi-uri osaamisen-hankkimistapa))
     :tutkinnonosa (koodiarvo tutkinnon-osa-koodi-uri)}))

(defn tutkinnonosat-by-hankkimistapa
  "Generates tutkinnosat by hankkimistapa map from existing HOKS. Such as:
  {:koulutussopimus [tutkinnonosat_123456]
   :oppisopimus [tutkinnonosat_234567 tutkinnonosat_345678]
   :oppilaitosmuotoinenkoulutus [tutkinnonosat_123456]}. Skips hankittavat
   paikalliset tutkinnon osat, because it does not have tutkinnonosa koodi."
  [hoks-id]
  (let [hoks (ehoks/get-hoks-by-id hoks-id)
        hato
        (mapcat #(map (osaamisen-hankkimistapa-mapper
                        (:tutkinnon-osa-koodi-uri %))
                      (:osaamisen-hankkimistavat %))
                (:hankittavat-ammat-tutkinnon-osat hoks))
        hyto
        (mapcat #(mapcat (fn [osa-alue]
                           (map (osaamisen-hankkimistapa-mapper
                                  (:tutkinnon-osa-koodi-uri %))
                                (:osaamisen-hankkimistavat osa-alue)))
                         (:osa-alueet %))
                (:hankittavat-yhteiset-tutkinnon-osat hoks))]
    (map-values (group-by :hankkimistapa (concat hato hyto))
                #(vec (set (map :tutkinnonosa %))))))

(defn create-and-save-kyselylinkki!
  "Hakee kyselylinkin arvosta, tallettaa sen tietokantaan,
  ja palauttaa osana herätettä."
  [herate opiskeluoikeus]
  ; päätellään alku ja loppupvm uudelleen, mikäli lähetys tapahtuukin myöhemmin
  ; kuin alunperin tallennettu alkupvm
  (let [alku-date (c/local-date-now)
        alkupvm   (str alku-date)
        loppupvm  (some-> (:heratepvm herate)
                          (LocalDate/parse)
                          (c/loppu alku-date)
                          (str))
        req-body (arvo/build-arvo-request-body
                   herate
                   opiskeluoikeus
                   (:request-id herate)
                   (:koulutustoimija herate)
                   (c/get-suoritus opiskeluoikeus)
                   alkupvm
                   loppupvm
                   (:odottaa-lahetysta c/kasittelytilat)
                   (tutkinnonosat-by-hankkimistapa (c/hoks-id herate)))
        arvo-resp (try (arvo/create-amis-kyselylinkki req-body)
                       (catch Exception e
                         (log/error e "Virhe kyselylinkin hakemisessa Arvosta."
                                    "Request:" req-body
                                    "Response:" (:body (ex-data e)))
                         (throw e)))]
    (if-let [kyselylinkki (:kysely_linkki arvo-resp)]
      (update-and-return-herate! herate {:kyselylinkki [:s kyselylinkki]
                                         :alkupvm [:s alkupvm]
                                         :voimassa-loppupvm [:s loppupvm]})
      (do (log/error "Arvo ei antanut kyselylinkkiä, heräte" herate)
          herate))))

(defn with-kyselylinkki!
  "Palauttaa herätteen, jossa on kyselylinkki mukana.
  Jos kyselylinkkiä ei valmiiksi ole, hakee kyselylinkin Arvosta,
  päivittää sen herätteeseen tietokantaan, ja lisää sen palautettavaan
  herätteeseen.  Jos kyselylinkki on jo olemassa tai jos sitä ei voitu luoda,
  palauttaa herätteen ilman muutoksia."
  [herate]
  (if (:kyselylinkki herate)
    herate
    (let [oo-oid (:opiskeluoikeus-oid herate)
          opiskeluoikeus (k/get-opiskeluoikeus-catch-404! oo-oid)
          [terminal? ext-funded?]
          (and opiskeluoikeus
               ((juxt c/terminaalitilassa? c/feedback-collecting-prevented?)
                opiskeluoikeus (:heratepvm herate)))]
      (cond
        (not opiskeluoikeus)
        (do
          (log/warn "Ei löytynyt opiskeluoikeutta" oo-oid)
          (update-and-return-herate!
            herate
            {:sms-lahetystila [:s (:ei-laheteta-oo-ei-loydy c/kasittelytilat)]
             :lahetystila [:s (:ei-laheteta-oo-ei-loydy c/kasittelytilat)]}))

        (or terminal? ext-funded?)
        (do
          (log/info "Palautteen kerääminen estetty"
                    (clojure.string/join
                      " ja " (concat (when terminal? ["opiskeluoikeuden tilan"])
                                     (when ext-funded? ["rahoituspohjan"])))
                    "vuoksi; opiskeluoikeus" (:oid opiskeluoikeus)
                    "ehoks-id" (c/hoks-id herate)
                    "herätepvm" (:heratepvm herate))
          (update-and-return-herate!
            herate
            {:lahetystila [:s (:ei-laheteta c/kasittelytilat)]
             :sms-lahetystila [:s (:ei-laheteta c/kasittelytilat)]}))

        :else
        (let [new-herate (create-and-save-kyselylinkki! herate opiskeluoikeus)]
          (update-kyselytunnus-in-ehoks! new-herate)
          new-herate)))))

(defn save-email-to-db
  "Tallentaa sähköpostin tiedot tietokantaan, kun sähköposti on lähetetty
  viestintäpalveluun."
  [herate id lahetyspvm]
  (try
    (ac/update-herate herate
                      {:lahetystila [:s (:viestintapalvelussa c/kasittelytilat)]
                       :viestintapalvelu-id [:n id]
                       :lahetyspvm [:s lahetyspvm]
                       :muistutukset [:n 0]})
    (catch AwsServiceException e
      (log/error "Tiedot herätteestä" herate "ei päivitetty kantaan")
      (log/error e))))

(defn update-data-in-ehoks
  "Päivittää sähköpostin tiedot ehoksiin, kun sähköposti on lähetetty
  viestintäpalveluun."
  [herate lahetyspvm]
  (try
    (c/send-lahetys-data-to-ehoks
      (:toimija_oppija herate)
      (:tyyppi_kausi herate)
      {:kyselylinkki (:kyselylinkki herate)
       :lahetyspvm lahetyspvm
       :sahkoposti (:sahkoposti herate)
       :lahetystila (:viestintapalvelussa c/kasittelytilat)})
    (catch Exception e
      (log/error "Virhe tietojen päivityksessä ehoksiin:" herate)
      (log/error e))))

(defn send-feedback-email
  "Lähettää palautekyselyviestin viestintäpalveluun."
  [herate]
  (try
    (vp/send-email {:subject (str "Palautetta oppilaitokselle - "
                                  "Respons till läroanstalten - "
                                  "Feedback to educational institution")
                    :body (vp/amispalaute-html herate)
                    :address (:sahkoposti herate)
                    :sender "Opetushallitus – Utbildningsstyrelsen – EDUFI"})
    (catch Exception e
      (log/error "Virhe palautesähköpostin lähetyksessä:" herate)
      (log/error e))))

(defn save-no-time-to-answer
  "Päivittää tietueen, jos herätteen vastausaika on umpeutunut."
  [herate]
  (try
    (ac/update-herate
      herate
      {:lahetystila [:s (:vastausaika-loppunut c/kasittelytilat)]
       :lahetyspvm  [:s (str (c/local-date-now))]})
    (catch Exception e
      (log/error "Virhe lähetystilan päivityksessä herätteelle,"
                 "jonka vastausaika umpeutunut:" herate)
      (log/error e))))

(defn do-query
  "Hakee tietueita tietokannasta, joiden lähetystilat ovat 'ei lähetetty'."
  []
  (ddb/query-items {:lahetystila [:eq [:s (:ei-lahetetty c/kasittelytilat)]]
                    :alkupvm     [:le [:s (str (c/local-date-now))]]}
                   {:index "lahetysIndex"}))

(defn send-email-for-palaute!
  "Lähettää sähköpostia yhden palauteherätteen suhteen (jos tarpeen)."
  [herate]
  (log/info "Käsitellään heräte:" herate)
  (try
    (let [kyselylinkki (:kyselylinkki herate)
          status (some-> kyselylinkki (arvo/get-kyselylinkki-status))]
      (cond
        (not kyselylinkki)
        (log/warn "Hoksille" (c/hoks-id herate)
                  "ei ole kyselylinkkiä, ei voi lähettää")

        (not status)
        (do (log/error "Kyselylinkin statuskysely epäonnistui linkille"
                       (:kyselylinkki herate))
            (throw (ex-info "Statuskysely epäonnistui" {:data herate})))

        (c/has-time-to-answer? (:voimassa_loppupvm status))
        (let [id (:id (send-feedback-email herate))
              lahetyspvm (str (c/local-date-now))]
          (log/info "Lähetetty sähköposti id" id ", tallennetaan tietokantaan")
          (save-email-to-db herate id lahetyspvm)
          (update-data-in-ehoks herate lahetyspvm))

        :else
        (do (log/info "Vastausaika loppunut hoksille" (c/hoks-id herate))
            (save-no-time-to-answer herate))))
    (catch Exception e
      (log/error e "at send-email-for-palaute! for" herate)
      (throw e))))

(defn -handleSendAMISEmails
  "Hakee lähetettäviä herätteitä tietokannasta ja lähettää viestit
  viestintäpalveluun."
  [_ event ^com.amazonaws.services.lambda.runtime.Context context]
  (log-caller-details-scheduled "handleSendAMISEmails" event context)
  (let [lahetettavat (do-query)
        timeout? (c/no-time-left? context 60000)]
    (log/info "Aiotaan käsitellä" (count lahetettavat) "lähetettävää viestiä.")
    (when (seq lahetettavat)
      (c/doseq-with-timeout
        timeout?
        [lahetettava lahetettavat]
        (try (-> lahetettava (with-kyselylinkki!) (send-email-for-palaute!))
             (catch Exception e (log/error e "herätteessä" lahetettava)))))))
