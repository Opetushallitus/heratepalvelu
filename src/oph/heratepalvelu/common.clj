(ns oph.heratepalvelu.common
  "Yhteiset funktiot herätepalvelulle."
  (:require [clj-time.coerce :as c]
            [clj-time.format :as f]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.ehoks :as ehoks]
            [oph.heratepalvelu.external.organisaatio :as org]
            [schema.core :as s])
  (:import (clojure.lang ExceptionInfo)
           (java.text Normalizer Normalizer$Form)
           (java.time LocalDate)
           (java.util UUID)))

(s/defschema herate-schema
  "AMIS-herätteen schema."
  {:ehoks-id                       s/Num
   :kyselytyyppi                   s/Str
   :opiskeluoikeus-oid             s/Str
   :oppija-oid                     s/Str
   :sahkoposti                     (s/constrained s/Str not-empty)
   (s/optional-key :puhelinnumero) (s/maybe s/Str)
   :alkupvm                        s/Str})

(def kasittelytilat
  "Heräteviestien lähetyksien käsittelytilat."
  {:ei-lahetetty "ei_lahetetty"
   :viestintapalvelussa "viestintapalvelussa"
   :vastausaika-loppunut "vastausaika_loppunut_ennen_lahetysta"
   :vastausaika-loppunut-m "vastausaika_loppunut_ennen_muistutusta"
   :vastattu "vastattu"
   :success "lahetetty"
   :failed "lahetys_epaonnistunut"
   :bounced "bounced"
   :ei-niputettu "ei_niputettu"
   :ei-niputeta "ei_niputeta"
   :niputusvirhe "niputusvirhe"
   :ei-laheteta "ei_laheteta"
   :ei-laheteta-oo-ei-loydy "ei_laheteta_opiskeluoikeus_ei_loydy"
   :ei-yhteystietoja "ei_kelvollisia_yhteystietoja"
   :ei-jaksoja "ei-jaksoja"
   :no-email "no-email"
   :no-phone "no-phone"
   :email-mismatch "email-mismatch"
   :phone-mismatch "phone-mismatch"
   :phone-invalid "phone-invalid"
   :queued "queued"})

(def herate-sources
  "Heräteviestien mahdolliset lähteet"
  {:ehoks "sqs_viesti_ehoksista"
   :koski "tiedot_muuttuneet_koskessa"})

(defn rand-str
  "Luo stringin, jossa on len randomisti valittua isoa kirjainta."
  [len]
  (str/join (repeatedly len #(char (+ (rand 26) 65)))))

(defn generate-uuid
  "Abstraktio UUID/randomUUID:n ympäri, joka helpottaa testaamista."
  []
  (str (UUID/randomUUID)))

(defn local-date-now
  "Abstraktio LocalDate/now:n ympäri, joka helpottaa testaamista."
  ^LocalDate []
  (LocalDate/now))

(defn has-time-to-answer?
  "Tarkistaa, onko aikaa jäljellä ennen annettua päivämäärää."
  [loppupvm]
  (when loppupvm
    (let [enddate (first (str/split loppupvm #"T"))]
      (not (.isBefore (LocalDate/parse enddate) (local-date-now))))))

(defn send-lahetys-data-to-ehoks
  "Lähettää lähetyksen tiedot ehoksiin."
  [toimija-oppija tyyppi-kausi data]
  (try
    (ehoks/add-lahetys-info-to-kyselytunnus data)
    (catch ExceptionInfo e
      (if (= 404 (:status (ex-data e)))
        (let [item (ddb/get-item {:toimija_oppija [:s toimija-oppija]
                                  :tyyppi_kausi [:s tyyppi-kausi]})]
          (try
            (ehoks/add-kyselytunnus-to-hoks
              (:ehoks-id item)
              (assoc data
                     :alkupvm (:alkupvm item)
                     :tyyppi (:kyselytyyppi item)))
            (catch ExceptionInfo e
              (if (= 404 (:status (ex-data e)))
                (log/warn "Ei hoksia " (:ehoks-id item))
                (throw e)))))
        (throw e)))))

(defn date-string-to-timestamp
  "Muuttaa stringinä olevan päivämäärän timestampiksi (long)."
  ([date-str fmt]
   (c/to-long (f/parse (fmt f/formatters)
                       date-str)))
  ([date-str]
   (date-string-to-timestamp date-str :date)))

(defn get-koulutustoimija-oid
  "Hakee koulutustoimijan OID:n opiskeluoikeudesta, tai organisaatiopalvelusta
  jos sitä ei löydy opiskeluoikeudesta."
  [opiskeluoikeus]
  (if-let [koulutustoimija-oid (:oid (:koulutustoimija opiskeluoikeus))]
    koulutustoimija-oid
    (do
      (log/info "Ei koulutustoimijaa opiskeluoikeudessa "
                (:oid opiskeluoikeus) ", haetaan Organisaatiopalvelusta")
      (:parentOid (org/get-organisaatio
                    (get-in opiskeluoikeus [:oppilaitos :oid]))))))

(defn kausi
  "Palauttaa kauden, johon annettu päivämäärä kuuluu. Kausi kestää heinäkuusta
  seuraavan vuoden kesäkuuhun."
  [alkupvm]
  (let [[year month] (str/split alkupvm #"-")]
    (if (> (Integer/parseInt month) 6)
      (str year "-" (inc (Integer/parseInt year)))
      (str (dec (Integer/parseInt year)) "-" year))))

(defn check-suoritus-type?
  "Varmistaa, että suorituksen tyyppi on joko ammatillinen tutkinto tai
  osittainen ammatillinen tutkinto."
  [suoritus]
  (or (= (:koodiarvo (:tyyppi suoritus)) "ammatillinentutkinto")
      (= (:koodiarvo (:tyyppi suoritus)) "ammatillinentutkintoosittainen")))

(defn check-opiskeluoikeus-suoritus-types?
  "Varmistaa, että opiskeluoikeuden suorituksiin kuuluu vähintään yksi, jonka
  tyyppi on ammatillinen tutkinto tai osittainen ammatillinen tutkinto."
  [opiskeluoikeus]
  (if (some check-suoritus-type?
            (:suoritukset opiskeluoikeus))
    true
    (log/info "Väärä suoritustyyppi opiskeluoikeudessa" (:oid opiskeluoikeus))))

(defn check-sisaltyy-opiskeluoikeuteen?
  "Palauttaa true, jos opiskeluoikeus EI sisälly toiseen opiskeluoikeuteen."
  [opiskeluoikeus]
  (if (:sisältyyOpiskeluoikeuteen opiskeluoikeus)
    (log/warn "Opiskeluoikeus"
              (:oid opiskeluoikeus)
              "sisältyy toiseen opiskeluoikeuteen.")
    true))

(defn get-suoritus
  "Hakee tutkinnon tai tutkinnon osan suorituksen opiskeluoikeudesta."
  [opiskeluoikeus]
  (reduce
    (fn [_ suoritus]
      (when (check-suoritus-type? suoritus)
        (reduced suoritus)))
    nil (:suoritukset opiskeluoikeus)))

(defn has-nayttotutkintoonvalmistavakoulutus?
  "Tarkistaa, onko opiskeluoikeudessa näyttötutkintoon valmistavan koulutuksen
  suoritus."
  [opiskeluoikeus]
  (some (fn [suoritus]
          (= (:koodiarvo (:tyyppi suoritus))
             "nayttotutkintoonvalmistavakoulutus"))
        (:suoritukset opiskeluoikeus)))

(defn next-niputus-date
  "Palauttaa seuraavan niputuspäivämäärän annetun päivämäärän jälkeen.
  Niputuspäivämäärät ovat kuun ensimmäinen ja kuudestoista päivä."
  ^LocalDate [pvm-str]
  (let [[^int year ^int month ^int day] (map #(Integer/parseInt %)
                                             (str/split pvm-str #"-"))]
    (if (< day 16)
      (LocalDate/of year month 16)
      (cond
        (= 6 month) (LocalDate/of year 6 30)
        (= 12 month) (LocalDate/of (inc year) 1 1)
        :else (LocalDate/of year (inc month) 1)))))

(defn- deaccent-string
  "Poistaa diakriittiset merkit stringistä ja palauttaa muokatun stringin."
  [utf8-string]
  (str/replace (Normalizer/normalize utf8-string Normalizer$Form/NFD)
               #"\p{InCombiningDiacriticalMarks}+"
               ""))

(defn normalize-string
  "Muuttaa muut merkit kuin kirjaimet ja numerot alaviivaksi."
  [string]
  (str/lower-case (str/replace (deaccent-string string) #"\W+" "_")))

(defn create-nipputunniste
  "Luo nipulle tunnisteen ilman erikoismerkkejä."
  [tyopaikan-nimi]
  (str (normalize-string tyopaikan-nimi) "_" (local-date-now) "_" (rand-str 6)))

(defn check-organisaatio-whitelist?
  "Tarkistaa, onko koulutustoimija mukana automaatiossa."
  [koulutustoimija timestamp]
  (let [item (ddb/get-item {:organisaatio-oid [:s koulutustoimija]}
                           (:orgwhitelist-table env))]
    (if (or (.isBefore (LocalDate/of 2021 6 30)
                       (LocalDate/ofEpochDay (/ timestamp 86400000)))
            (and (:kayttoonottopvm item)
                 (<= (c/to-long (f/parse (:date f/formatters)
                                         (:kayttoonottopvm item)))
                     timestamp)
                 (<= (c/to-long (f/parse (:date f/formatters)
                                         (:kayttoonottopvm item)))
                     (c/to-long (local-date-now)))))
      true
      (log/info "Koulutustoimija" koulutustoimija
                "ei ole mukana automaatiossa, tai herätepvm"
                (str (LocalDate/ofEpochDay (/ timestamp 86400000)))
                "on ennen käyttöönotto päivämäärää"))))

(defn check-duplicate-herate?
  "Palauttaa true, jos ei ole vielä herätettä tallennettua tietokantaan samoilla
  koulutustoimijalla, oppijalla, tyypillä ja laskentakaudella, tai jos olemassa
  olevan herätteen saa ylikirjoittaa uuden herätteen tiedoilla. Heräte
  ylikirjoitetaan, jos uudet tiedot tulevat ehoksista, tai jos olemassaolevan
  herätteen tiedot tulivat Koskesta. Herätettä ei ylikirjoiteta, jos
  kyselylinkki on jo muodostunut."
  [oppija toimija kausi kyselytyyppi herate-source]
  (if (let [check-db?
            (fn [tyyppi]
              (let [existing (ddb/get-item
                               {:toimija_oppija [:s (str toimija "/" oppija)]
                                :tyyppi_kausi [:s (str tyyppi "/" kausi)]})]
                (and (or (empty? existing)
                         (= (:herate-source existing) (:koski herate-sources))
                         (= herate-source (:ehoks herate-sources)))
                     (nil? (:kyselylinkki existing)))))]
        (if (or (= kyselytyyppi "tutkinnon_suorittaneet")
                (= kyselytyyppi "tutkinnon_osia_suorittaneet"))
          (and (check-db? "tutkinnon_suorittaneet")
               (check-db? "tutkinnon_osia_suorittaneet"))
          (check-db? kyselytyyppi)))
    true
    (log/info "Ei ylikirjoiteta olemassaolevaa herätettä. Oppija:" oppija
              "koulutustoimija:" toimija ";" kyselytyyppi kausi)))

(defn delete-other-paattoherate
  "";; TODO
  [oppija koulutustoimija laskentakausi kyselytyyppi]
  (when (or (= kyselytyyppi "tutkinnon_suorittaneet")
            (= kyselytyyppi "tutkinnon_osia_suorittaneet"))
    (let [tyyppi (if (= kyselytyyppi "tutkinnon_suorittaneet")
                   "tutkinnon_osia_suorittaneet"
                   "tutkinnon_suorittaneet")]
      (ddb/delete-item {:toimija_oppija [:s (str koulutustoimija "/" oppija)]
                        :tyyppi_kausi   [:s (str tyyppi "/" laskentakausi)]}))))

(defn check-valid-herate-date
  "Varmistaa, että herätteen päivämäärä ei ole ennen 1.7.2021."
  [heratepvm]
  (try
    (not (.isAfter (LocalDate/of 2021 7 1) (LocalDate/parse heratepvm)))
    (catch Exception e
      (log/error e))))

(def herate-checker
  "Herätescheman tarkistusfunktio."
  (s/checker herate-schema))

(defn alku
  "Laskee vastausajan alkupäivämäärän: annettu päivämäärä jos se on vielä
  tulevaisuudessa; muuten tämä päivä."
  [^LocalDate herate-date]
  (if (.isAfter herate-date (local-date-now))
    herate-date
    (local-date-now)))

(defn loppu
  "Laskee vastausajan loppupäivämäärän: 30 päivän päästä (inklusiivisesti),
  mutta ei myöhempi kuin 60 päivää (inklusiivisesti) herätepäivän jälkeen."
  [^LocalDate herate ^LocalDate alku]
  (let [last (.plusDays herate 59)
        normal (.plusDays alku 29)]
    (if (.isBefore last normal)
      last
      normal)))

(defn- make-set-pair
  "Luo '#x = :x' -pareja update-expreja varten."
  [item-key]
  (let [normalized (normalize-string (name item-key))]
    (str "#" normalized " = :" normalized)))

(defn create-update-item-options
  "Luo options-objekti update-item -kutsulle."
  [updates]
  {:update-expr (str "SET " (str/join ", " (map make-set-pair (keys updates))))
   :expr-attr-names (reduce #(assoc %1 (str "#" (normalize-string %2)) %2)
                            {}
                            (map name (keys updates)))
   :expr-attr-vals
   (reduce-kv #(assoc %1 (str ":" (normalize-string (name %2))) %3)
              {}
              updates)})

(defn is-before
  "Wrapper .isBefore-metodin ympäri, jolla on tyyppianotaatiot."
  [^LocalDate one-date ^LocalDate other-date]
  (.isBefore one-date other-date))

(defn is-after
  "Wrapper .isAfter-metodin ympäri, jolla on tyyppianotaatiot."
  [^LocalDate one-date ^LocalDate other-date]
  (.isAfter one-date other-date))
