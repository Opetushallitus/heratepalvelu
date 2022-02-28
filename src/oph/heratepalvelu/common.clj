(ns oph.heratepalvelu.common
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
             {:ehoks-id           s/Num
              :kyselytyyppi       s/Str
              :opiskeluoikeus-oid s/Str
              :oppija-oid         s/Str
              :sahkoposti         (s/constrained s/Str not-empty)
              :alkupvm            s/Str})

(def kasittelytilat
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
   :ei-yhteystietoja "ei_kelvollisia_yhteystietoja"
   :ei-jaksoja "ei-jaksoja"
   :no-email "no-email"
   :no-phone "no-phone"
   :email-mismatch "email-mismatch"
   :phone-mismatch "phone-mismatch"
   :phone-invalid "phone-invalid"
   :queued "queued"})

(defn rand-str
  "Luo stringin, jossa on len randomisti valittua isoa kirjainta."
  [len]
  (apply str (take len (repeatedly #(char (+ (rand 26) 65))))))

(defn generate-uuid
  "Abstraktio UUID/randomUUID:n ympäri, joka helpottaa testaamista."
  []
  (str (UUID/randomUUID)))

(defn to-date
  "Parsii yyyy-MM-dd -muotoisen päivämäärän ja palauttaa LocalDate."
  [str-date]
  (let [[year month day] (map #(Integer. %1) (str/split str-date #"-"))]
    (LocalDate/of year month day)))

(defn local-date-now
  "Abstraktio LocalDate/now:n ympäri, joka helpottaa testaamista."
  []
  (LocalDate/now))

(defn has-time-to-answer?
  "Tarkistaa, onko aikaa jäljellä ennen annettua päivämäärää."
  [loppupvm]
  (when loppupvm
    (let [enddate (first (str/split loppupvm #"T"))]
      (not (.isBefore (to-date enddate) (local-date-now))))))

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
      (:parentOid
        (org/get-organisaatio
          (get-in opiskeluoikeus [:oppilaitos :oid]))))))

(defn kausi
  "Palauttaa kauden, johon annettu päivämäärä kuuluu. Kausi kestää heinäkuusta
  seuraavan vuoden kesäkuuhun."
  [alkupvm]
  (let [[year month] (str/split alkupvm #"-")]
    (if (> (Integer/parseInt month) 6)
      (str year "-" (+ (Integer/parseInt year) 1))
      (str (- (Integer/parseInt year) 1) "-" year))))

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
    (log/info "Väärä suoritustyyppi opiskeluoikeudessa " (:oid opiskeluoikeus))))

(defn check-sisaltyy-opiskeluoikeuteen?
  "Palauttaa true, jos opiskeluoikeus EI sisälly toiseen opiskeluoikeuteen."
  [opiskeluoikeus]
  (if (:sisältyyOpiskeluoikeuteen opiskeluoikeus)
    (log/warn "Opiskeluoikeus " (:oid opiskeluoikeus) " sisältyy toiseen opiskeluoikeuteen.")
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
          (= (:koodiarvo (:tyyppi suoritus)) "nayttotutkintoonvalmistavakoulutus"))
        (:suoritukset opiskeluoikeus)))

(defn next-niputus-date
  "Palauttaa seuraavan niputuspäivämäärän annetun päivämäärän jälkeen.
  Niputuspäivämäärät ovat kuun ensimmäinen ja kuudestoista päivä."
  [pvm-str]
  (let [[year month day] (map
                           #(Integer. %)
                           (str/split pvm-str #"-"))]
    (if (< day 16)
      (LocalDate/of year month 16)
      (if (= 12 month)
        (LocalDate/of (+ year 1) 1 1)
        (LocalDate/of year (+ month 1) 1)))))

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
    (if (.isBefore (LocalDate/of 2021 6 30) (LocalDate/ofEpochDay (/ timestamp 86400000)))
      true
      (if
        (and
          (:kayttoonottopvm item)
          (<= (c/to-long (f/parse (:date f/formatters) (:kayttoonottopvm item)))
              timestamp)
          (<= (c/to-long (f/parse (:date f/formatters) (:kayttoonottopvm item)))
              (c/to-long (local-date-now))))
        true
        (log/info "Koulutustoimija " koulutustoimija " ei ole mukana automaatiossa,"
                  " tai herätepvm " (str (LocalDate/ofEpochDay (/ timestamp 86400000)))
                  " on ennen käyttöönotto päivämäärää")))))

(defn check-duplicate-herate?
  "Palauttaa true, jos ei ole vielä herätettä tallennettua tietokantaan samoilla
  koulutustoimijalla, oppijalla, tyypillä ja laskentakaudella."
  [oppija koulutustoimija laskentakausi kyselytyyppi]
  (if
    (let [check-db?
          (fn [tyyppi]
            (empty? (ddb/get-item
                      {:toimija_oppija [:s (str koulutustoimija "/" oppija)]
                       :tyyppi_kausi [:s (str tyyppi "/" laskentakausi)]})))]
      (if (or (= kyselytyyppi "tutkinnon_suorittaneet")
              (= kyselytyyppi "tutkinnon_osia_suorittaneet"))
        (and (check-db? "tutkinnon_suorittaneet")
             (check-db? "tutkinnon_osia_suorittaneet"))
        (check-db? kyselytyyppi)))
    true
    (log/warn "Tämän kyselyn linkki on jo toimituksessa oppilaalle "
              oppija " koulutustoimijalla " koulutustoimija
              "(tyyppi '" kyselytyyppi "' kausi " laskentakausi ")")))

(defn check-valid-herate-date
  "Varmistaa, että herätteen päivämäärä ei ole ennen 1.7.2021."
  [heratepvm]
  (try
    (not (.isAfter (LocalDate/of 2021 7 1) (LocalDate/parse heratepvm)))
   (catch Exception e
     (log/error e))))

(def herate-checker
  (s/checker herate-schema))

(defn alku
  "Laskee vastausajan alkupäivämäärän: annettu päivämäärä jos se on vielä
  tulevaisuudessa; muuten tämä päivä."
  [herate-date]
  (if (.isAfter herate-date (local-date-now))
    herate-date
    (local-date-now)))

(defn loppu
  "Laskee vastausajan loppupäivämäärän: 30 päivän päästä (inklusiivisesti),
  mutta ei myöhempi kuin 60 päivää (inklusiivisesti) herätepäivän jälkeen."
  [herate alku]
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
