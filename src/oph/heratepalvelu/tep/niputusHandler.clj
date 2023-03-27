(ns oph.heratepalvelu.tep.niputusHandler
  "Käsittelee alustavia nippuja ja luo niille kyselylinkeille."
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.external.ehoks :as ehoks]
            [oph.heratepalvelu.external.koski :as koski]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.tep.tepCommon :as tc])
  (:import (clojure.lang ExceptionInfo)
           (com.amazonaws.services.lambda.runtime Context)
           (java.lang Math)
           (java.time DayOfWeek LocalDate)
           (java.time.temporal ChronoUnit)
           (software.amazon.awssdk.awscore.exception AwsServiceException)
           (software.amazon.awssdk.services.dynamodb.model
             ConditionalCheckFailedException)))

(gen-class
  :name "oph.heratepalvelu.tep.niputusHandler"
  :methods [[^:static handleNiputus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn not-in-keskeytymisajanjakso?
  "Varmistaa, että annettu päivämäärä ei kuulu keskeytymisajanjaksoon."
  [^LocalDate date keskeytymisajanjaksot]
  (or (empty? keskeytymisajanjaksot)
      (every? #(or (and (:alku %) (.isBefore date (:alku %)))
                   (and (:loppu %) (.isAfter date (:loppu %))))
              keskeytymisajanjaksot)))

; (defn not-before-or-after-opiskeluoikeus?
;   "Varmistaa, että jakson alkamis- ja päättymispäivät eivät ole
;   opiskeluoikeuden alku- ja loppuajankohdan ulkopuolella."
;   [start end oo-tilat]
;   (when-not (empty? oo-tilat)
;     (let [oo-alku  (:alku (first oo-tilat))
;           oo-loppu (:loppu (last oo-tilat))]
;       (and (or (.isAfter start oo-alku) (.isEqual start oo-alku))
;            (or (not (:loppu (last oo-tilat)))
;                (or (.isBefore end oo-loppu) (.isEqual end oo-loppu)))))))

(defn is-weekday?
  "Tarkistaa, onko annettu päivämäärä arkipäivä."
  [^LocalDate date]
  (not (or (= (.getDayOfWeek date) DayOfWeek/SATURDAY)
           (= (.getDayOfWeek date) DayOfWeek/SUNDAY))))

(defn filtered-jakso-days
  "Luo listan jakson arkipäivistä LocalDate:ina."
  [jakso]
  (filter is-weekday?
          (let [start (LocalDate/parse (:jakso_alkupvm jakso))
                end (LocalDate/parse (:jakso_loppupvm jakso))]
            (map #(.plusDays start %)
                 (range (inc (.between ChronoUnit/DAYS start end)))))))

(defn convert-ajanjakso
  "Muuntaa ajanjakson alku- ja loppupäivät LocalDate:iksi."
  [jakso]
  (cond-> jakso
    (:alku jakso)  (assoc :alku  (LocalDate/parse (:alku jakso)))
    (:loppu jakso) (assoc :loppu (LocalDate/parse (:loppu jakso)))))

(defn add-to-jaksot-by-day
  "Lisää jaksoon viittaavan referenssin jokaiselle päivälle jaksot-by-day
  -objektissa, jolloin jakso on voimassa eikä ole keskeytynyt. Objekti
  jaksot-by-day on map LocalDate-päivämääristä sekvensseihin jaksoista, jotka
  ovat voimassa ja keskeytymättömiä sinä päivänä."
  [jaksot-by-day jakso opiskeluoikeus]
  (let [oo-tilat (->> (:opiskeluoikeusjaksot (:tila opiskeluoikeus))
                      (map convert-ajanjakso)
                      (sort-by :alku #(compare %2 %1)) ; reverse order
                      (reduce
                        #(cons
                           (if (first %1)
                             (assoc %2 :loppu (.minusDays (:alku (first %1)) 1))
                             %2)
                           %1)
                        []))
        kjaksot-parsed (map convert-ajanjakso
                            (:keskeytymisajanjaksot jakso))
        kjaksot-oo (filter #(or (= "valiaikaisestikeskeytynyt"
                                   (:koodiarvo (:tila %)))
                                (= "loma" (:koodiarvo (:tila %))))
                           oo-tilat)
        kjaksot (concat kjaksot-parsed kjaksot-oo)]
    (reduce #(if (not-in-keskeytymisajanjakso? %2 kjaksot)
               (assoc %1 %2 (cons jakso (get %1 %2)))
               %1)
            jaksot-by-day
            (filtered-jakso-days jakso))))

(defn add-to-jaksot-by-day-new
  "Lisää jaksoon viittaavan referenssin jokaiselle päivälle jaksot-by-day
  -objektissa, jolloin jakso on voimassa eikä ole keskeytynyt. Objekti
  jaksot-by-day on map LocalDate-päivämääristä sekvensseihin jaksoista, jotka
  ovat voimassa ja keskeytymättömiä sinä päivänä."
  [jaksot-by-day jakso opiskeluoikeus]
  (let [oo-tilat (->> (:opiskeluoikeusjaksot (:tila opiskeluoikeus))
                      (map convert-ajanjakso)
                      (sort-by :alku #(compare %2 %1)) ; reverse order
                      (reduce
                        #(cons
                           (if (first %1)
                             (assoc %2 :loppu (.minusDays (:alku (first %1)) 1))
                             %2)
                           %1)
                        []))
        kjaksot-parsed (map convert-ajanjakso
                            (:keskeytymisajanjaksot jakso))
        kjaksot-oo (filter #(or (= "valiaikaisestikeskeytynyt"
                                   (:koodiarvo (:tila %))))
                           oo-tilat)
        kjaksot (concat kjaksot-parsed kjaksot-oo)]
    (reduce #(if (not-in-keskeytymisajanjakso? %2 kjaksot)
               (assoc %1 %2 (cons jakso (get %1 %2)))
               %1)
            jaksot-by-day
            (let [start (LocalDate/parse (:jakso_alkupvm jakso))
                  end (LocalDate/parse (:jakso_loppupvm jakso))]
              (map #(.plusDays start %)
                   (range (inc (.between ChronoUnit/DAYS start end))))))))

(defn get-osa-aikaisuus
  "Hakee osa-aikaisuutta jaksosta ja varmistaa, että se on sallittujen rajojen
  sisällä."
  [jakso]
  (if (and (some? (:osa_aikaisuus jakso))
           (pos? (:osa_aikaisuus jakso))
           (> 100 (:osa_aikaisuus jakso)))
    (:osa_aikaisuus jakso)
    100))

(defn handle-one-day
  "Jakaa yhden päivän aikaa silloin keskeytymättömien jaksojen välillä."
  [jaksot]
  (let [fraction (/ 1.0 (count jaksot))]
    (into {} (map #(vector (:hankkimistapa_id %)
                           (/ (* fraction (get-osa-aikaisuus %)) 100))
                  jaksot))))

(defn handle-one-day-new
  "Jakaa yhden päivän aikaa silloin keskeytymättömien jaksojen välillä."
  [jaksot]
  (let [fraction (/ 1.0 (count jaksot))]
    (into {} (map #(assoc {} (:hankkimistapa_id %)
                          {:with-oa (/ (* fraction (get-osa-aikaisuus %)) 100)
                           :without-oa fraction}) ; ilman osa-aikaisuustietoa
                  jaksot))))

(defn compute-kestot
  "Laskee jaksojen kestot ja palauttaa mapin OHT ID:stä kestoihin. Olettaa, että
  kaikki jaksot kuuluvat samalle oppilaalle."
  [jaksot]
  (let [first-start-date  (first (sort (map :jakso_alkupvm jaksot)))
        last-end-date     (first (reverse (sort (map :jakso_loppupvm jaksot))))
        concurrent-jaksot (ehoks/get-tyoelamajaksot-active-between
                            (:oppija_oid (first jaksot))
                            first-start-date
                            last-end-date)
        ; FIXME: zipmap
        oo-map (reduce #(assoc %1 %2 (koski/get-opiskeluoikeus-catch-404 %2))
                       {}
                       (set (map :opiskeluoikeus_oid concurrent-jaksot)))
        do-one #(add-to-jaksot-by-day %1
                                      %2
                                      (get oo-map (:opiskeluoikeus_oid %2)))]
    ; FIXME: mapcat + merge-with
    (reduce (fn [acc m] (reduce-kv #(assoc %1 %2 (+ %3 (get %1 %2 0.0))) acc m))
            {}
            (map handle-one-day (vals (reduce do-one {} concurrent-jaksot))))))

(defn get-jaksojen-opiskeluoikeudet
  "Funktiossa kokeillaan ensin hakea jaksojen opiskeluoikeuksia
  `opiskeluoikeudet` mapista. Jos niitä ei löydy tästä, ne haetaan Koskesta."
  [opiskeluoikeudet opiskeluoikeus-oidt]
  (zipmap opiskeluoikeus-oidt
          (map #(or (get opiskeluoikeudet %)
                    (koski/get-opiskeluoikeus-catch-404 %))
               opiskeluoikeus-oidt)))

(defn compute-kesto-old
  "Laskee yksittäisen jakson keston."
  [jakso concurrent-jaksot opiskeluoikeudet]
  (let [do-one #(add-to-jaksot-by-day
                  %1
                  %2
                  (get opiskeluoikeudet (:opiskeluoikeus_oid %2)))]
    (get (reduce
           (fn [acc m] (reduce-kv #(assoc %1 %2 (+ %3 (get %1 %2 0.0))) acc m))
           {}
           (map handle-one-day (vals (reduce do-one {} concurrent-jaksot))))
         (:hankkimistapa_id jakso))))

(defn compute-kesto-new
  "Laskee yksittäisen jakson keston."
  [jakso concurrent-jaksot opiskeluoikeudet]
  (let [do-one #(add-to-jaksot-by-day-new
                  %1 %2 (get opiskeluoikeudet (:opiskeluoikeus_oid %2)))]
    (get (reduce
           (fn [acc m]
             (reduce-kv #(assoc %1 %2 (merge-with + %3 (get %1 %2))) acc m))
           {}
           (map handle-one-day-new (vals (reduce do-one {} concurrent-jaksot))))
         (:hankkimistapa_id jakso))))

(defn compute-kesto-old-and-new
  "Laskee yksittäisen jakson keston, vanhalla ja uudella tavalla."
  [jakso concurrent-jaksot opiskeluoikeudet]
  (let [cc-jaksot-with-oo
        (filter #(get opiskeluoikeudet (:opiskeluoikeus_oid %1))
                concurrent-jaksot)]
    {:vanha (compute-kesto-old jakso cc-jaksot-with-oo opiskeluoikeudet)
     :uusi  (compute-kesto-new jakso cc-jaksot-with-oo opiskeluoikeudet)}))

(defn compute-kestot-new
  "Laskee kestot kaikille jaksoille `jaksot` listassa."
  [jaksot]
  (loop [kestot {}
         opiskeluoikeudet {}
         jaksot jaksot]
    (if-let [jakso (first jaksot)]
      (let [concurrent-jaksot (ehoks/get-tyoelamajaksot-active-between
                                (:oppija_oid jakso)
                                (:jakso_alkupvm jakso)
                                (:jakso_loppupvm jakso))
            jaksojen-opiskeluoikeudet
            (get-jaksojen-opiskeluoikeudet opiskeluoikeudet
                                           (map :opiskeluoikeus_oid
                                                concurrent-jaksot))]
        (recur (assoc kestot
                      (:hankkimistapa_id jakso)
                      (compute-kesto-old-and-new
                        jakso concurrent-jaksot jaksojen-opiskeluoikeudet))
               (merge opiskeluoikeudet jaksojen-opiskeluoikeudet)
               (rest jaksot)))
      kestot)))

(defn group-jaksot-and-compute-kestot
  "Ryhmittää jaksot oppija_oid:n perusteella ja laskee niiden kestot."
  [jaksot]
  (->> (group-by :oppija_oid jaksot)
       (vals)
       (map (comp compute-kestot reverse))
       (apply merge)))

(defn query-jaksot
  "Hakee tietokannasta ne jaksot, jotka kuuluvat annettuun nippuun ja joiden
  viimeiset vastauspäivämäärät eivät ole menneisyydessä."
  [nippu]
  (ddb/query-items
    {:ohjaaja_ytunnus_kj_tutkinto
     [:eq [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]]
     :niputuspvm [:eq [:s (:niputuspvm nippu)]]}
    {:index "niputusIndex"
     :filter-expression "#pvm >= :pvm AND attribute_exists(#tunnus)"
     :expr-attr-names {"#pvm"    "viimeinen_vastauspvm"
                       "#tunnus" "tunnus"}
     :expr-attr-vals {":pvm" [:s (str (c/local-date-now))]}}
    (:jaksotunnus-table env)))

(defn math-round "Wrapper Math/round:in ympäri" [^double x] (Math/round x))

(defn retrieve-and-update-jaksot
  "Hakee nippuun kuuluvat jaksot tietokannasta, laskee niiden kestot, päivittää
  kestotiedot tietokantaan, ja palauttaa päivitetyt jaksot."
  [nippu]
  (let [jaksot (query-jaksot nippu)
        kestot (group-jaksot-and-compute-kestot jaksot)]
    (map #(let [kesto (math-round (get kestot (:hankkimistapa_id %) 0.0))]
            (tc/update-jakso % {:kesto [:n kesto]})
            (assoc % :kesto kesto))
         jaksot)))

(defn niputa
  "Luo nippukyselylinkin jokaiselle alustavalle nipulle, jos sillä on vielä
  jaksoja, joilla on vielä aikaa vastata."
  [nippu]
  (log/info "Niputetaan " nippu)
  (let [request-id (c/generate-uuid)
        jaksot (retrieve-and-update-jaksot nippu)
        tunnukset (vec (map (fn [x] {:tunnus                (:tunnus x)
                                     :tyopaikkajakson_kesto (:kesto x)})
                            jaksot))]
    (if (not-empty tunnukset)
      (let [tunniste (c/create-nipputunniste (:tyopaikan_nimi (first jaksot)))
            arvo-resp (arvo/create-nippu-kyselylinkki
                        (arvo/build-niputus-request-body
                          tunniste nippu tunnukset request-id))]
        (if (some? (:nippulinkki arvo-resp))
          (try
            (tc/update-nippu
              nippu
              {:kasittelytila    [:s (:ei-lahetetty c/kasittelytilat)]
               :kyselylinkki     [:s (:nippulinkki arvo-resp)]
               :voimassaloppupvm [:s (:voimassa_loppupvm arvo-resp)]
               :request_id       [:s request-id]
               :kasittelypvm     [:s (str (c/local-date-now))]}
              {:cond-expr "attribute_not_exists(kyselylinkki)"})
            (catch ConditionalCheckFailedException e
              (log/warn "Nipulla"
                        (:ohjaaja_ytunnus_kj_tutkinto nippu)
                        "on jo kantaan tallennettu kyselylinkki.")
              (arvo/delete-nippukyselylinkki tunniste))
            (catch AwsServiceException e
              (log/error "Virhe DynamoDB tallennuksessa " e)
              (arvo/delete-nippukyselylinkki tunniste)
              (throw e)))
          (do (log/error "Virhe niputuksessa " nippu request-id)
              (log/error arvo-resp)
              (tc/update-nippu
                nippu
                {:kasittelytila [:s (:niputusvirhe c/kasittelytilat)]
                 :reason        [:s (str (or (:errors arvo-resp)
                                             "no reason in response"))]
                 :request_id    [:s request-id]
                 :kasittelypvm  [:s (str (c/local-date-now))]}))))
      (do (log/warn "Ei jaksoja, joissa vastausaikaa jäljellä"
                    (:ohjaaja_ytunnus_kj_tutkinto nippu)
                    (:niputuspvm nippu))
          (tc/update-nippu nippu
                           {:kasittelytila [:s (:ei-jaksoja c/kasittelytilat)]
                            :request_id    [:s request-id]
                            :kasittelypvm  [:s (str (c/local-date-now))]})))))

(defn do-query
  "Hakee käsiteltäviä nippuja tietokannasta."
  []
  (ddb/query-items {:kasittelytila [:eq [:s (:ei-niputettu c/kasittelytilat)]]
                    :niputuspvm    [:le [:s (str (c/local-date-now))]]}
                   {:index "niputusIndex"
                    :limit 10}
                   (:nippu-table env)))

(defn get-nippu-key
  "Luo memoisointiavaimen nipulle."
  [nippu]
  {:ohjaaja_ytunnus_kj_tutkinto (:ohjaaja_ytunnus_kj_tutkinto nippu)
   :niputuspvm                  (:niputuspvm nippu)})

(defn -handleNiputus
  "Hakee ja niputtaa niputtamattomat jaksot."
  [_ event ^com.amazonaws.services.lambda.runtime.Context context]
  (log-caller-details-scheduled "handleNiputus" event context)
  (let [processed-niput (atom {})]
    (loop [niputettavat (sort-by :niputuspvm #(- (compare %1 %2)) (do-query))]
      (log/info "Käsitellään" (count niputettavat) "niputusta.")
      (when (seq niputettavat)
        (doseq [nippu niputettavat]
          (when-not (get @processed-niput (get-nippu-key nippu))
            (niputa nippu)
            (swap! processed-niput assoc (get-nippu-key nippu) true)))
        (when (< 120000 (.getRemainingTimeInMillis context))
          (recur (do-query)))))))
