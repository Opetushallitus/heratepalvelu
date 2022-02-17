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
  (:import (java.util UUID)
           (clojure.lang ExceptionInfo)
           (java.time LocalDate)))

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

(defn rand-str [len]
  (apply str (take len (repeatedly #(char (+ (rand 26) 65))))))

(defn generate-uuid []
  (str (UUID/randomUUID)))

(defn to-date [str-date]
  (let [[year month day] (map #(Integer. %1) (str/split str-date #"-"))]
    (LocalDate/of year month day)))

(defn local-date-now []
  "Abstraktio LocalDate/now:n ympäri, joka helpottaa testaamista."
  (LocalDate/now))

(defn has-time-to-answer? [loppupvm]
  (when loppupvm
    (let [enddate (first (str/split loppupvm #"T"))]
      (not (.isBefore (to-date enddate) (local-date-now))))))

(defn send-lahetys-data-to-ehoks [toimija-oppija tyyppi-kausi data]
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
  ([date-str fmt]
   (c/to-long (f/parse (fmt f/formatters)
                       date-str)))
  ([date-str]
   (date-string-to-timestamp date-str :date)))

(defn get-koulutustoimija-oid [opiskeluoikeus]
  (if-let [koulutustoimija-oid (:oid (:koulutustoimija opiskeluoikeus))]
    koulutustoimija-oid
    (do
      (log/info "Ei koulutustoimijaa opiskeluoikeudessa "
                (:oid opiskeluoikeus) ", haetaan Organisaatiopalvelusta")
      (:parentOid
        (org/get-organisaatio
          (get-in opiskeluoikeus [:oppilaitos :oid]))))))

(defn kausi [alkupvm]
  (let [[year month] (str/split alkupvm #"-")]
    (if (> (Integer/parseInt month) 6)
      (str year "-" (+ (Integer/parseInt year) 1))
      (str (- (Integer/parseInt year) 1) "-" year))))

(defn check-suoritus-type? [suoritus]
  (or (= (:koodiarvo (:tyyppi suoritus)) "ammatillinentutkinto")
      (= (:koodiarvo (:tyyppi suoritus)) "ammatillinentutkintoosittainen")))

(defn check-opiskeluoikeus-suoritus-types? [opiskeluoikeus]
  (if (some check-suoritus-type?
            (:suoritukset opiskeluoikeus))
    true
    (log/info "Väärä suoritustyyppi opiskeluoikeudessa " (:oid opiskeluoikeus))))

(defn check-sisaltyy-opiskeluoikeuteen? [opiskeluoikeus]
  (if (:sisältyyOpiskeluoikeuteen opiskeluoikeus)
    (log/warn "Opiskeluoikeus " (:oid opiskeluoikeus) " sisältyy toiseen opiskeluoikeuteen.")
    true))

(defn get-suoritus [opiskeluoikeus]
  "Haetaan tutkinto/tutkinnon osa suoritus"
  (reduce
    (fn [_ suoritus]
      (when (check-suoritus-type? suoritus)
        (reduced suoritus)))
    nil (:suoritukset opiskeluoikeus)))

(defn has-nayttotutkintoonvalmistavakoulutus? [opiskeluoikeus]
  (some (fn [suoritus]
          (= (:koodiarvo (:tyyppi suoritus)) "nayttotutkintoonvalmistavakoulutus"))
        (:suoritukset opiskeluoikeus)))

(defn next-niputus-date [pvm-str]
  (let [[year month day] (map
                           #(Integer. %)
                           (str/split pvm-str #"-"))]
    (if (< day 16)
      (LocalDate/of year month 16)
      (if (= 12 month)
        (LocalDate/of (+ year 1) 1 1)
        (LocalDate/of year (+ month 1) 1)))))

(defn- deaccent-string [str]
  (let [normalized (java.text.Normalizer/normalize str java.text.Normalizer$Form/NFD)]
    (str/replace normalized #"\p{InCombiningDiacriticalMarks}+" "")))

(defn normalize-string [string]
  (str/lower-case (str/replace (deaccent-string string) #"\W+" "_")))

(defn create-nipputunniste [tyopaikan-nimi]
  "Luo nipulle tunnisteen ilman erikoismerkkejä"
  (str (normalize-string tyopaikan-nimi) "_" (local-date-now) "_" (rand-str 6)))

(defn check-organisaatio-whitelist?
  ([koulutustoimija]
   (let [item (ddb/get-item {:organisaatio-oid [:s koulutustoimija]}
                            (:orgwhitelist-table env))]
     (if (.isBefore (LocalDate/of 2021 6 30) (local-date-now))
       true
       (if
         (and
           (:kayttoonottopvm item)
           (<= (c/to-long (f/parse (:date f/formatters) (:kayttoonottopvm item)))
               (c/to-long (local-date-now))))
         true
         (log/info "Koulutustoimija " koulutustoimija " ei ole mukana automaatiossa")))))
  ([koulutustoimija timestamp]
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
                   " on ennen käyttöönotto päivämäärää"))))))

(defn check-duplicate-herate? [oppija koulutustoimija laskentakausi kyselytyyppi]
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

(defn check-valid-herate-date [heratepvm]
  (try
    (not (.isAfter (LocalDate/of 2021 7 1) (LocalDate/parse heratepvm)))
   (catch Exception e
     (log/error e))))

(def herate-checker
  (s/checker herate-schema))

(defn alku [herate-date]
  (if (.isAfter herate-date (local-date-now))
    herate-date
    (local-date-now)))

(defn loppu [herate alku]
  (let [last (.plusDays herate 59)
        normal (.plusDays alku 29)]
    (if (.isBefore last normal)
      last
      normal)))
