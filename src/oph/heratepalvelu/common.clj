(ns oph.heratepalvelu.common
  (:require [clj-time.format :as f]
            [clj-time.coerce :as c]
            [clj-time.core :as t]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.organisaatio :refer [get-organisaatio]]
            [oph.heratepalvelu.external.arvo :refer :all]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.ehoks :refer :all]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [schema.core :as s])
  (:import (java.util UUID)
           (software.amazon.awssdk.awscore.exception AwsServiceException)
           (software.amazon.awssdk.services.dynamodb.model ConditionalCheckFailedException)
           (clojure.lang ExceptionInfo)))

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
   :vastattu "vastattu_tai_vastausaika_loppunut"
   :success "success"
   :failed "failed"
   :bounced "bounced"
   :ei-niputettu "ei_niputettu"
   :yhdistetty "yhdistetty"})

(defn rand-str [len]
  (apply str (take len (repeatedly #(char (+ (rand 26) 65))))))

(defn generate-uuid []
  (.toString (UUID/randomUUID)))

(defn has-time-to-answer? [loppupvm]
  (when loppupvm
    (let [enddate (first (str/split loppupvm #"T"))
          [years months days] (map #(Integer. %)
                                   (str/split enddate #"-"))]
      (not (t/before? (t/local-date years months days) (t/today))))))

(defn send-lahetys-data-to-ehoks [toimija-oppija tyyppi-kausi data]
  (try
    (add-lahetys-info-to-kyselytunnus data)
    (catch ExceptionInfo e
      (if (= 404 (:status (ex-data e)))
        (let [item (ddb/get-item {:toimija_oppija [:s toimija-oppija]
                                  :tyyppi_kausi [:s tyyppi-kausi]})]
          (add-kyselytunnus-to-hoks
            (:ehoks-id item)
            (assoc data
              :alkupvm (:alkupvm item)
              :tyyppi (:kyselytyyppi item))))
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
        (get-organisaatio
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

(defn next-niputus-date [pvm]
  (let [[year month day] (map
                           #(Integer. %)
                           (str/split pvm #"-"))]
    (if (< day 16)
      (t/local-date year month 16)
      (if (= 12 month)
        (t/local-date (+ year 1) 1 1)
        (t/local-date year (+ month 1) 1)))))

(defn previous-niputus-date [pvm]
  (let [[year month day] (map
                           #(Integer. %)
                           (str/split pvm #"-"))]
    (if (< day 16)
      (t/nth-day-of-the-month year month 1)
      (t/nth-day-of-the-month
        (t/minus
          (t/local-date year month day)
          (t/months 1))
        16))))

(defn check-organisaatio-whitelist?
  ([koulutustoimija]
   (let [item (ddb/get-item {:organisaatio-oid [:s koulutustoimija]}
                            (:orgwhitelist-table env))]
     (if
       (and
         (:kayttoonottopvm item)
         (<= (c/to-long (f/parse (:date f/formatters) (:kayttoonottopvm item)))
             (c/to-long (t/today))))
       true
       (log/info "Koulutustoimija " koulutustoimija " ei ole mukana automaatiossa"))))
  ([koulutustoimija timestamp]
   (let [item (ddb/get-item {:organisaatio-oid [:s koulutustoimija]}
                            (:orgwhitelist-table env))]
     (if
       (and
         (:kayttoonottopvm item)
         (<= (c/to-long (f/parse (:date f/formatters) (:kayttoonottopvm item)))
             timestamp)
         (<= (c/to-long (f/parse (:date f/formatters) (:kayttoonottopvm item)))
             (c/to-long (t/today))))
       true
       (log/info "Koulutustoimija " koulutustoimija " ei ole mukana automaatiossa,"
                 " tai herätepvm on ennen käyttöönotto päivämäärää")))))

(defn check-duplicate-herate? [oppija koulutustoimija laskentakausi kyselytyyppi]
  (if
    (empty? (ddb/get-item {:toimija_oppija [:s (str koulutustoimija "/" oppija)]
                           :tyyppi_kausi [:s (str kyselytyyppi "/" laskentakausi)]}))
    true
    (log/warn "Tämän kyselyn linkki on jo toimituksessa oppilaalle "
              oppija " koulutustoimijalla " koulutustoimija
              "(tyyppi '" kyselytyyppi "' kausi " laskentakausi ")")))

(def herate-checker
  (s/checker herate-schema))

(defn save-herate [herate opiskeluoikeus]
  (log/info "Kerätään tietoja " (:ehoks-id herate) " " (:kyselytyyppi herate))
  (if (some? (herate-checker herate))
    (log/error {:herate herate :msg (herate-checker herate)})
    (let [kyselytyyppi (:kyselytyyppi herate)
          alkupvm (:alkupvm herate)
          koulutustoimija (get-koulutustoimija-oid opiskeluoikeus)
          suoritus (get-suoritus opiskeluoikeus)
          oppija (str (:oppija-oid herate))
          laskentakausi (kausi alkupvm)
          uuid (generate-uuid)
          oppilaitos (:oid (:oppilaitos opiskeluoikeus))
          suorituskieli (str/lower-case
                          (:koodiarvo (:suorituskieli suoritus)))]
      (when
        (check-duplicate-herate?
          oppija koulutustoimija laskentakausi kyselytyyppi)
        (let [arvo-resp (create-amis-kyselylinkki
                          (build-arvo-request-body
                            herate
                            opiskeluoikeus
                            uuid
                            koulutustoimija
                            suoritus))
              voimassa-loppupvm (:voimassa_loppupvm arvo-resp)]
          (if-let [kyselylinkki (:kysely_linkki arvo-resp)]
            (try
              (log/info "Tallennetaan kantaan " (str koulutustoimija "/" oppija)
                        " " (str kyselytyyppi "/" laskentakausi))
              (ddb/put-item {:toimija_oppija      [:s (str koulutustoimija "/" oppija)]
                             :tyyppi_kausi        [:s (str kyselytyyppi "/" laskentakausi)]
                             :kyselylinkki        [:s kyselylinkki]
                             :sahkoposti          [:s (:sahkoposti herate)]
                             :suorituskieli       [:s suorituskieli]
                             :lahetystila         [:s (:ei-lahetetty kasittelytilat)]
                             :alkupvm             [:s alkupvm]
                             :request-id          [:s uuid]
                             :oppilaitos          [:s oppilaitos]
                             :ehoks-id            [:n (str (:ehoks-id herate))]
                             :opiskeluoikeus-oid  [:s (:oid opiskeluoikeus)]
                             :oppija-oid          [:s oppija]
                             :koulutustoimija     [:s koulutustoimija]
                             :kyselytyyppi        [:s kyselytyyppi]
                             :rahoituskausi       [:s laskentakausi]
                             :viestintapalvelu-id [:n "-1"]
                             :voimassa-loppupvm   [:s (or voimassa-loppupvm "-")]
                             :tallennuspvm        [:s (str (t/today))]}
                            {:cond-expr (str "attribute_not_exists(toimija_oppija) AND "
                                             "attribute_not_exists(tyyppi_kausi)")})
              (try
                (add-kyselytunnus-to-hoks (:ehoks-id herate)
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
                           :voimassa-loppupvm     voimassa-loppupvm}))
              (catch ConditionalCheckFailedException e
                (log/warn "Tämän kyselyn linkki on jo toimituksessa oppilaalle "
                          oppija " koulutustoimijalla " koulutustoimija
                          "(tyyppi " kyselytyyppi " kausi " (kausi alkupvm) ")"
                          "Deaktivoidaan kyselylinkki, request-id " uuid)
                (delete-amis-kyselylinkki kyselylinkki))
              (catch AwsServiceException e
                (log/error "Virhe tietokantaan tallennettaessa " kyselylinkki " " uuid)
                (delete-amis-kyselylinkki kyselylinkki)
                (throw e))
              (catch Exception e
                (delete-amis-kyselylinkki kyselylinkki)
                (log/error "Unknown error " e)
                (throw e)))))))))
