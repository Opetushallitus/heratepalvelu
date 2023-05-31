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
           (com.google.i18n.phonenumbers PhoneNumberUtil NumberParseException)
           (java.text Normalizer Normalizer$Form)
           (java.time LocalDate)
           (java.time.format DateTimeParseException)
           (java.util UUID)))

(s/defschema herate-schema-base
  {:ehoks-id                       s/Num
   :kyselytyyppi                   s/Str
   :opiskeluoikeus-oid             s/Str
   :oppija-oid                     s/Str
   (s/optional-key :sahkoposti)    (s/maybe s/Str)
   (s/optional-key :puhelinnumero) (s/maybe s/Str)
   :alkupvm                        s/Str})

(s/defschema herate-schema
  "AMIS-herätteen schema."
  (s/constrained herate-schema-base
                 #(not (and (= (:kyselytyyppi %) "aloittaneet")
                            (empty? (:sahkoposti %))))
                 "Aloituskyselyn herätteessä sahkoposti on pakollinen tieto"))

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

(defn current-time-millis
  "Abstraktio System/currentTimeMillis:n ympäri, joka helpottaa testaamista."
  ^Integer []
  (System/currentTimeMillis))

(defn local-date-now
  "Abstraktio LocalDate/now:n ympäri, joka helpottaa testaamista."
  ^LocalDate []
  (LocalDate/now))

(defn period-contains-date?
  "Tarkistaa, onko annettu päivämäärä ainakin yhden annetun aikajakson sisällä."
  [periods date]
  (let [date-string (str date)]
    (boolean
      (some #(and (or (not (:alku %)) (>= (compare date-string (:alku %)) 0))
                  (or (not (:loppu %)) (<= (compare date-string (:loppu %)) 0)))
            periods))))

(defn is-maksuton?
  "Tarkistaa, onko kyseessä oleva opiskeluoikeus maksuton."
  [opiskeluoikeus date]
  (period-contains-date?
    (filter :maksuton (:maksuttomuus (:lisätiedot opiskeluoikeus)))
    date))

(defn erityinen-tuki-voimassa?
  "Tarkistaa, saako opiskelija erityistä tukea opiskeluoikeuden perusteella."
  [opiskeluoikeus date]
  (period-contains-date? (:erityinenTuki (:lisätiedot opiskeluoikeus)) date))

(defn get-rahoitusryhma
  "Päättää, mihin rahoitusryhmään oppilas kuuluu. Oppilas kuuluu rahoitusryhmään
  1 jos hänen opinnot ovat maksuttomia tai hän on erityisen tuen opiskelijan."
  [opiskeluoikeus ^LocalDate herate-date]
  (if (or (is-maksuton? opiskeluoikeus (str herate-date))
          (erityinen-tuki-voimassa? opiskeluoikeus (str herate-date)))
    "01"
    "02"))

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

(defn ammatillinen-tutkinto?
  "Varmistaa, että suorituksen tyyppi on joko ammatillinen tutkinto tai
  osittainen ammatillinen tutkinto."
  [suoritus]
  (some? (#{"ammatillinentutkinto" "ammatillinentutkintoosittainen"}
          (:koodiarvo (:tyyppi suoritus)))))

(defn has-one-or-more-ammatillinen-tutkinto?
  "Varmistaa, että opiskeluoikeuden suorituksiin kuuluu vähintään yksi, jonka
  tyyppi on ammatillinen tutkinto tai osittainen ammatillinen tutkinto."
  [opiskeluoikeus]
  (some? (or (some ammatillinen-tutkinto? (:suoritukset opiskeluoikeus))
             (log/info "Väärä suoritustyyppi opiskeluoikeudessa"
                       (:oid opiskeluoikeus)))))

(defn sisaltyy-toiseen-opiskeluoikeuteen?
  "Palauttaa true, jos opiskeluoikeus sisältyy toiseen opiskeluoikeuteen."
  [opiskeluoikeus]
  (some? (when (:sisältyyOpiskeluoikeuteen opiskeluoikeus)
           (log/warn "Opiskeluoikeus"
                     (:oid opiskeluoikeus)
                     "sisältyy toiseen opiskeluoikeuteen.")
           true)))

(defn get-suoritus
  "Hakee tutkinnon tai tutkinnon osan suorituksen opiskeluoikeudesta."
  [opiskeluoikeus]
  (first (filter ammatillinen-tutkinto? (:suoritukset opiskeluoikeus))))

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

(defn whitelisted-organisaatio?!
  "Tarkistaa, onko koulutustoimija mukana automaatiossa."
  [koulutustoimija timestamp]
  (let [item (ddb/get-item {:organisaatio-oid [:s koulutustoimija]}
                           (:orgwhitelist-table env))]
    (some?
      (or (.isBefore (LocalDate/of 2021 6 30)
                     (LocalDate/ofEpochDay (/ timestamp 86400000)))
          (and (:kayttoonottopvm item)
               (<= (c/to-long (f/parse (:date f/formatters)
                                       (:kayttoonottopvm item)))
                   timestamp)
               (<= (c/to-long (f/parse (:date f/formatters)
                                       (:kayttoonottopvm item)))
                   (c/to-long (local-date-now))))
          (log/info "Koulutustoimija" koulutustoimija
                    "ei ole mukana automaatiossa, tai herätepvm"
                    (str (LocalDate/ofEpochDay (/ timestamp 86400000)))
                    "on ennen käyttöönotto päivämäärää")))))

(defn already-superseding-herate!
  "Jos on jo heräte tallennettua tietokantaan samoilla koulutustoimijalla,
  oppijalla, tyypillä ja laskentakaudella, eikä sitä saa ylikirjoittaa
  uuden herätteen tiedoilla, palauttaa kyseisen herätteen, muuten nil.
  eHOKSista tulleen herätteen tietoja ei saa ylikirjoittaa Koskesta tulevan
  herätteen tiedoilla, eikä herätettä saa korvata,
  jos kyselylinkki on jo muodostunut."
  [oppija toimija kausi kyselytyyppi herate-source]
  (let [superseding-herate-for-kyselytyyppi
        (fn [tyyppi]
          (let [existing (ddb/get-item
                           {:toimija_oppija [:s (str toimija "/" oppija)]
                            :tyyppi_kausi [:s (str tyyppi "/" kausi)]})]
            (when (or (:kyselylinkki existing)
                      (and (= (:herate-source existing) (:ehoks herate-sources))
                           (= herate-source (:koski herate-sources))))
              existing)))]
    (if (#{"tutkinnon_suorittaneet" "tutkinnon_osia_suorittaneet"} kyselytyyppi)
      (or (superseding-herate-for-kyselytyyppi "tutkinnon_suorittaneet")
          (superseding-herate-for-kyselytyyppi "tutkinnon_osia_suorittaneet"))
      (superseding-herate-for-kyselytyyppi kyselytyyppi))))

(defn delete-other-paattoherate
  "Jos heräte on päättöheräte, poistaa tietokannasta kaikki eri tyyppiset
  päättöherätteet (esim. jos heräte on tutkinnon_suorittaneet; poistaa olemassa
  olevan tutkinnon_osia_suorittaneet -herätteen)."
  [oppija koulutustoimija laskentakausi kyselytyyppi]
  (when (or (= kyselytyyppi "tutkinnon_suorittaneet")
            (= kyselytyyppi "tutkinnon_osia_suorittaneet"))
    (let [tyyppi (if (= kyselytyyppi "tutkinnon_suorittaneet")
                   "tutkinnon_osia_suorittaneet"
                   "tutkinnon_suorittaneet")]
      (ddb/delete-item {:toimija_oppija [:s (str koulutustoimija "/" oppija)]
                        :tyyppi_kausi   [:s (str tyyppi "/" laskentakausi)]}))))

(defn valid-herate-date?
  "onko herätteen päivämäärä aikaisintaan 1.7.2022?"
  [heratepvm]
  (try
    (not (.isAfter (LocalDate/of 2022 7 1) (LocalDate/parse (or heratepvm ""))))
    (catch DateTimeParseException e
      (log/warn "Bad date" heratepvm)
      false)))

(def herate-schema-errors
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
   :expr-attr-names
   (let [names (map name (keys updates))]
     (zipmap (map #(str "#" (normalize-string %)) names) names))
   :expr-attr-vals
   ; FIXME: map-keys
   (reduce-kv #(assoc %1 (str ":" (normalize-string (name %2))) %3)
              {}
              updates)})

(defn alku-and-loppu-to-localdate
  "Muuntaa parametrina annetun hashmapin :alku ja :loppu -avaimien
  merkkijonomuotoiset päivämäärät LocalDate:iksi."
  [jakso]
  (cond-> jakso
    (:alku jakso)  (update :alku  #(LocalDate/parse %))
    (:loppu jakso) (update :loppu #(LocalDate/parse %))))

(defn is-before
  "Wrapper .isBefore-metodin ympäri, jolla on tyyppianotaatiot."
  [^LocalDate one-date ^LocalDate other-date]
  (.isBefore one-date other-date))

(defn is-after
  "Wrapper .isAfter-metodin ympäri, jolla on tyyppianotaatiot."
  [^LocalDate one-date ^LocalDate other-date]
  (.isAfter one-date other-date))

(defn valid-number?
  "Sallii vain numeroita, jotka kirjasto luokittelee mobiilinumeroiksi tai
  mahdollisiksi mobiilinumeroiksi (FIXED_LINE_OR_MOBILE). Jos funktio ei hyväksy
  numeroa, jonka tiedät olevan validi, tarkista, miten kirjasto luokittelee sen:
  https://libphonenumber.appspot.com/."
  [number]
  (try
    (let [utilobj (PhoneNumberUtil/getInstance)
          numberobj (.parse utilobj number "FI")]
      (and (empty? (filter (fn [^Character x] (Character/isLetter x)) number))
           (.isValidNumber utilobj numberobj)
           (let [numtype (str (.getNumberType utilobj numberobj))]
             (or (= numtype "FIXED_LINE_OR_MOBILE") (= numtype "MOBILE")))))
    (catch NumberParseException e
      (log/error "PhoneNumberUtils failed to parse phonenumber")
      (log/error e)
      false)))

(defn client-error?
  "Tarkistaa, onko virheen statuskoodi 4xx-haitarissa."
  [e]
  (and (> (:status (ex-data e)) 399)
       (< (:status (ex-data e)) 500)))

(defn get-opiskeluoikeusjakso-for-date
  "Hakee opiskeluoikeudesta jakson, joka on voimassa tiettynä päivänä."
  [opiskeluoikeus vahvistus-pvm mode]
  (let [offset (if (= mode :one-day-offset) 1 0)
        jaksot (sort-by :alku (:opiskeluoikeusjaksot (:tila opiskeluoikeus)))]
    (reduce (fn [res next]
              (if (>= (compare vahvistus-pvm (:alku next)) offset)
                next
                (reduced res)))
            (first jaksot)
            jaksot)))

(defn get-tila
  "Hakee opiskeluoikeuden tilan tiettynä päivänä."
  [opiskeluoikeus vahvistus-pvm]
  (-> opiskeluoikeus
      (get-opiskeluoikeusjakso-for-date vahvistus-pvm :normal)
      (get-in [:tila :koodiarvo])))

(def terminaalitilat
  #{"eronnut" "katsotaaneronneeksi" "mitatoity" "peruutettu"
    "valiaikaisestikeskeytynyt"})

(defn terminaalitilassa?
  "Palauttaa true, jos opiskeluoikeus on terminaalitilassa (eronnut,
  katsotaan eronneeksi, mitätöity, peruutettu, tai väliaikaisesti keskeytynyt),
  myös kun opiskeluoikeus on siirtynyt tähän tilaan juuri kysyttynä päivänä."
  [opiskeluoikeus loppupvm]
  (let [jakso (get-opiskeluoikeusjakso-for-date
                opiskeluoikeus loppupvm :one-day-offset)
        tila (get-in jakso [:tila :koodiarvo])]
    (some? (when (terminaalitilat tila)
             (log/warn "Opiskeluoikeus"
                       (:oid opiskeluoikeus)
                       "terminaalitilassa"
                       tila)
             true))))

(defn get-oppilaitokset
  "Hakee oppilaitosten nimet organisaatiopalvelusta jaksojen oppilaiton-kentän
  perusteella."
  [jaksot]
  (try
    (seq (set (map #(:nimi (org/get-organisaatio (:oppilaitos %1))) jaksot)))
    (catch Exception e
      (log/error "Virhe kutsussa organisaatiopalveluun")
      (log/error e))))

(def feedback-collecting-preventing-codes #{"6" "14" "15"})

(defn feedback-collecting-prevented?
  "Jätetäänkö palaute keräämättä sen vuoksi, että opiskelijan opiskelu on
  tällä hetkellä rahoitettu muilla rahoituslähteillä?"
  [opiskeluoikeus heratepvm]
  (-> opiskeluoikeus
      (get-opiskeluoikeusjakso-for-date heratepvm :normal)
      (get-in [:opintojenRahoitus :koodiarvo])
      (feedback-collecting-preventing-codes)
      (some?)))
