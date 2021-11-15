(ns oph.heratepalvelu.common
  (:require [clj-time.format :as f]
            [clj-time.coerce :as c]
            [clj-time.core :as t]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.organisaatio :as org]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.ehoks :as ehoks]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [schema.core :as s])
  (:import (java.util UUID)
           (software.amazon.awssdk.awscore.exception AwsServiceException)
           (software.amazon.awssdk.services.dynamodb.model ConditionalCheckFailedException)
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
   :ei-laheteta "ei_laheteta"
   :ei-yhteystietoja "ei_kelvollisia_yhteystietoja"
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

(defn has-time-to-answer? [loppupvm]
  (when loppupvm
    (let [enddate (first (str/split loppupvm #"T"))]
      (not (.isBefore (to-date enddate) (LocalDate/now))))))

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
  (str (normalize-string tyopaikan-nimi) "_" (t/today) "_" (rand-str 6)))

(defn check-organisaatio-whitelist?
  ([koulutustoimija]
   (let [item (ddb/get-item {:organisaatio-oid [:s koulutustoimija]}
                            (:orgwhitelist-table env))]
     (if (.isBefore (LocalDate/of 2021 6 30) (LocalDate/now))
       true
       (if
         (and
           (:kayttoonottopvm item)
           (<= (c/to-long (f/parse (:date f/formatters) (:kayttoonottopvm item)))
               (c/to-long (t/today))))
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
               (c/to-long (t/today))))
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
  (if (.isAfter herate-date (LocalDate/now))
    herate-date
    (LocalDate/now)))

(defn loppu [herate alku]
  (let [last (.plusDays herate 59)
        normal (.plusDays alku 29)]
    (if (.isBefore last normal)
      last
      normal)))

(defn save-herate [herate opiskeluoikeus koulutustoimija]
  (log/info "Kerätään tietoja " (:ehoks-id herate) " " (:kyselytyyppi herate))
  (if (some? (herate-checker herate))
    (log/error {:herate herate :msg (herate-checker herate)})
    (let [kyselytyyppi (:kyselytyyppi herate)
          heratepvm (:alkupvm herate)
          herate-date (to-date heratepvm)
          alku-date (alku herate-date)
          alkupvm   (str alku-date)
          loppu-date (loppu herate-date alku-date)
          loppupvm  (str loppu-date)
          suoritus (get-suoritus opiskeluoikeus)
          oppija (str (:oppija-oid herate))
          laskentakausi (kausi heratepvm)
          uuid (generate-uuid)
          oppilaitos (:oid (:oppilaitos opiskeluoikeus))
          suorituskieli (str/lower-case
                          (:koodiarvo (:suorituskieli suoritus)))]
      (when
        (check-duplicate-herate?
          oppija koulutustoimija laskentakausi kyselytyyppi)
        (let [req-body (arvo/build-arvo-request-body
                         herate
                         opiskeluoikeus
                         uuid
                         koulutustoimija
                         suoritus
                         alkupvm
                         loppupvm)
              arvo-resp (if (= kyselytyyppi "aloittaneet")
                          (arvo/create-amis-kyselylinkki
                            req-body)
                          (arvo/create-amis-kyselylinkki-catch-404
                            req-body))]
          (if-let [kyselylinkki (:kysely_linkki arvo-resp)]
            (try
              (log/info "Tallennetaan kantaan" (str koulutustoimija "/" oppija)
                         (str kyselytyyppi "/" laskentakausi) ", request-id: " uuid)
              (ddb/put-item {:toimija_oppija      [:s (str koulutustoimija "/" oppija)]
                             :tyyppi_kausi        [:s (str kyselytyyppi "/" laskentakausi)]
                             :kyselylinkki        [:s kyselylinkki]
                             :sahkoposti          [:s (:sahkoposti herate)]
                             :suorituskieli       [:s suorituskieli]
                             :lahetystila         [:s (:ei-lahetetty kasittelytilat)]
                             :alkupvm             [:s alkupvm]
                             :heratepvm           [:s heratepvm]
                             :request-id          [:s uuid]
                             :oppilaitos          [:s oppilaitos]
                             :ehoks-id            [:n (str (:ehoks-id herate))]
                             :opiskeluoikeus-oid  [:s (:oid opiskeluoikeus)]
                             :oppija-oid          [:s oppija]
                             :koulutustoimija     [:s koulutustoimija]
                             :kyselytyyppi        [:s kyselytyyppi]
                             :rahoituskausi       [:s laskentakausi]
                             :viestintapalvelu-id [:n "-1"]
                             :voimassa-loppupvm   [:s loppupvm]
                             :tutkintotunnus      [:s (str (:tutkintotunnus req-body))]
                             :osaamisala          [:s (str (:osaamisala req-body))]
                             :toimipiste-oid      [:s (str (:toimipiste_oid req-body))]
                             :hankintakoulutuksen-toteuttaja
                                                  [:s (str (:hankintakoulutuksen_toteuttaja
                                                             req-body))]
                             :tallennuspvm        [:s (str (LocalDate/now))]}
                            {:cond-expr (str "attribute_not_exists(toimija_oppija) AND "
                                             "attribute_not_exists(tyyppi_kausi)")})
              (try
                (ehoks/add-kyselytunnus-to-hoks (:ehoks-id herate)
                                          {:kyselylinkki kyselylinkki
                                           :tyyppi       kyselytyyppi
                                           :alkupvm      alkupvm
                                           :lahetystila  (:ei-lahetetty kasittelytilat)})
                (catch Exception e
                  (log/error "Virhe linkin lähetyksessä eHOKSiin " e)))
              (when (has-nayttotutkintoonvalmistavakoulutus? opiskeluoikeus)
                (log/info {:nayttotutkinto        true
                           :hoks-id               (:ehoks-id herate)
                           :opiskeluoikeus-oid    (:oid opiskeluoikeus)
                           :koulutuksenjarjestaja koulutustoimija
                           :tutkintotunnus        (get-in suoritus [:koulutusmoduuli
                                                                    :tunniste
                                                                    :koodiarvo])
                           :kyselytunnus          (last (str/split kyselylinkki #"/"))
                           :voimassa-loppupvm     loppupvm}))
              (catch ConditionalCheckFailedException _
                (log/warn "Tämän kyselyn linkki on jo toimituksessa oppilaalle "
                          oppija " koulutustoimijalla " koulutustoimija
                          "(tyyppi " kyselytyyppi " kausi " laskentakausi ")"
                          "Deaktivoidaan kyselylinkki, request-id " uuid)
                (arvo/delete-amis-kyselylinkki kyselylinkki))
              (catch AwsServiceException e
                (log/error "Virhe tietokantaan tallennettaessa " kyselylinkki " " uuid)
                (arvo/delete-amis-kyselylinkki kyselylinkki)
                (throw e))
              (catch Exception e
                (arvo/delete-amis-kyselylinkki kyselylinkki)
                (log/error "Unknown error " e)
                (throw e)))
            (log/error "Ei kyselylinkkiä arvon palautteessa" arvo-resp)))))))
