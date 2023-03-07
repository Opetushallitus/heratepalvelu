(ns oph.heratepalvelu.tep.niputusHandler
  "Käsittelee alustavia nippuja ja luo niille kyselylinkeille."
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.util.date :as d]
            [oph.heratepalvelu.util.string :as s]
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

(defn round-vals [m] (reduce-kv #(assoc %1 %2 (Math/round %3)) {} m))

(defn not-in-keskeytymisajanjakso?
  "Varmistaa, että annettu päivämäärä ei kuulu keskeytymisajanjaksoon."
  [^LocalDate date keskeytymisajanjaksot]
  (or (empty? keskeytymisajanjaksot)
      (every? #(or (and (:alku %) (.isBefore date (:alku %)))
                   (and (:loppu %) (.isAfter date (:loppu %))))
              keskeytymisajanjaksot)))

(defn filtered-jakso-days
  "Luo listan jakson arkipäivistä LocalDate:ina."
  [jakso]
  (filter d/weekday?
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

(defn calculate-kestot-old
  "DEPRECATED: Vanha työpaikkajakson keston laskentatapa.
  Laskee oppijan jaksojen kestot. Funktio olettaa, että `jaksot` pitävät
  sisällään ainoastaan yhden oppijan jaksoja. Jakson kesto voi pyöristyä
  nollaan, mikäli kokonaiskesto on jotain 0 ja 0.5 väliltä."
  [oppijan-jaksot opiskeluoikeudet]
  (let [do-one #(add-to-jaksot-by-day
                  %1 %2 (get opiskeluoikeudet (:opiskeluoikeus_oid %2)))]
    (round-vals
      (reduce (fn [acc m] (reduce-kv #(assoc %1 %2 (+ %3 (get %1 %2 0.0))) acc m))
              {}
              (map handle-one-day (vals (reduce do-one {} oppijan-jaksot)))))))

; Alla on vaihtoehtoinen tapa laskea kestot vanhalla laskentatavalla. Erona
; yllä olevaan tapaan on se, että alla oleva antaa jakson kestoksi nolla, jos
; jaksolla ei ole opiskeluoikeutta.

; (defn calculate-kestot-old
;   [jaksot opiskeluoikeudet]
;   (let [; Harmonisoidaan :jakso_alkupvm ja :jakso_loppupvm avaimet
;         ; avaimiksi :alku ja :loppu myöhempää prosessointia varten.
;         ; Muutetaan vastaavat päivämäärät myös LocalDate-objekteiksi.
;         jaksot (map (comp convert-ajanjakso harmonize-date-keys) jaksot)]
;     (round-vals
;       (merge-with
;         * ; Kerrotaan kestot osa-aikaisuuskertoimilla
;         (apply merge-with
;                + ; Summataan yksittäisten päivien kestot
;                ; Alustetaan kaikki kestot nollaksi:
;                (cons (zipmap (map :hankkimistapa_id jaksot) (repeat 0))
;                      (map (partial calculate-single-day-kestot
;                                    jaksot
;                                    opiskeluoikeudet)
;                           ; Huomioidaan vain päivät maanantaista perjantaihin.
;                           (filter d/weekday?
;                                   (d/range (apply d/earliest (map :alku jaksot))
;                                            (apply d/latest (map :loppu jaksot))))))
;                (get-osa-aikaisuus-kertoimet jaksot))))))

; (defn get-osa-aikaisuus-kertoimet
;   [jaksot]
;   (reduce #(assoc % (:hankkimistapa_id %2) (/ (get-osa-aikaisuus %2) 100))
;           {}
;           jaksot))

(defn get-opiskeluoikeusjaksot
  [opiskeluoikeus]
  (->> (:opiskeluoikeusjaksot (:tila opiskeluoikeus))
       (map convert-ajanjakso)
       (sort-by :alku #(compare %2 %1)) ; reverse order
       ; Add :loppu dates to each ajanjakso
       (reduce
         #(cons (if (first %1)
                  (assoc %2 :loppu (.minusDays (:alku (first %1)) 1))
                  %2)
                %1)
         [])))

(defn get-keskeytymisajanjaksot
  [jakso opiskeluoikeus]
  (let [keskeytynyt-tilat #{"valiaikaisestikeskeytynyt"} ; "loma"}
        kjaksot-in-jakso (map convert-ajanjakso (:keskeytymisajanjaksot jakso))
        kjaksot-in-opiskeluoikeus
        (filter #(contains? keskeytynyt-tilat (:koodiarvo (:tila %)))
                (get-opiskeluoikeusjaksot opiskeluoikeus))]
  (concat kjaksot-in-jakso kjaksot-in-opiskeluoikeus)))

(defn in-jakso?
  "Tarkistaa sisältyykö päivämäärä `pvm` jaksoon `jakso`. Oletus on, että 
  sekä pvm että jakson avaimet :alku ja :loppu ovat tyyppiä `LocalDate`. 
  Palauttaa `true` jos päivämäärä sisältyy jaksoon, muuten `false`."
  [pvm jakso]
  (let [alku (:alku jakso) loppu (:loppu jakso)]
    (and (or (.isAfter pvm alku) (.isEqual pvm alku))
         (or (nil? loppu) (.isBefore pvm loppu) (.isEqual pvm loppu)))))

(defn jakso-active?
  "Tarkistaa, onko `jakso` aktiivinen päivämääränä `pvm`. Ts. funktiossa
  tarkistetaan, kuuluuko `pvm` jaksoon ja sisältyykö se mihinkään jakson tai
  opiskeluoikeuden tiedoissa olevaan keskeytymisajanjaksoon."
  [jakso opiskeluoikeus pvm]
  (and (in-jakso? pvm jakso)
       (some? opiskeluoikeus)
       (not-any? #(in-jakso? pvm %)
                 (get-keskeytymisajanjaksot jakso opiskeluoikeus))))

(defn calculate-single-day-kestot
  "Laskee aktiivisena olevien jaksojen kestot yhden päivän osalta, eli
  suorittaa niin sanotun 'jyvityksen'. Tällä tarkoitetaan sitä, että yhden
  päivän kesto jaetaan tasaisesti kaikille samanaikaisesti aktiivisena
  oleville jaksoille. Funktio palauttaa hashmapissa ainoastaan aktiivisena
  olevien jaksojen kestot."
  [jaksot opiskeluoikeudet pvm]
  (let [active-jaksos ; Koostetaan lista aktiivisten jaksojen id:istä.
        (reduce #(if (jakso-active?
                       %2 (get opiskeluoikeudet (:opiskeluoikeus_oid %2)) pvm)
                   (cons (:hankkimistapa_id %2) %)
                   %)
                []
                jaksot)
        kesto (/ 1.0 (count active-jaksos))]
    (zipmap active-jaksos (repeat kesto))))

(defn harmonize-date-keys
  "Harmonisoi jakson alku- ja loppupäivämääriä vastaavat avaimet, jotta
  kestonlaskennassa voidaan hyödyntää mahdollisimman paljon samoja funktiota."
  [jakso]
  (cond-> jakso
    (:jakso_alkupvm jakso)  (assoc :alku (:jakso_alkupvm jakso))
    (:jakso_loppupvm jakso) (assoc :loppu (:jakso_loppupvm jakso))))

(defn calculate-kestot
  "Laskee oppijan jaksojen kestot. Funktio olettaa, että `jaksot` pitävät
  sisällään ainoastaan yhden oppijan jaksoja. Yksittäisen jakson kesto voi olla
  nolla, jos kyseiselle jaksolle ei löydy opiskeluoikeutta Koskesta, Tällaista
  jaksoa ei kuitenkaan oteta jyvityksessä huomioon muiden jaksojen kestoja
  laskettaessa. Jakson kesto voi myös pyöristyä nollaan, mikäli kokonaiskesto
  on jotain 0 ja 0.5 väliltä."
  [oppijan-jaksot opiskeluoikeudet]
  (let [; Harmonisoidaan :jakso_alkupvm ja :jakso_loppupvm avaimet
        ; avaimiksi :alku ja :loppu myöhempää prosessointia varten.
        ; Muutetaan vastaavat päivämäärät myös LocalDate-objekteiksi.
        jaksot (map (comp convert-ajanjakso harmonize-date-keys)
                    oppijan-jaksot)]
    (round-vals
      (apply merge-with
             + ; Summataan yksittäisten päivien kestot
             ; Alustetaan kaikki kestot nollaksi:
             (cons (zipmap (map :hankkimistapa_id jaksot) (repeat 0.0))
                   (map (partial calculate-single-day-kestot
                                 jaksot
                                 opiskeluoikeudet)
                        (d/range (apply d/earliest (map :alku jaksot))
                                 (apply d/latest   (map :loppu jaksot)))))))))

(defn query-jaksot!
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

(defn get-concurrent-jaksot-from-ehoks!
  "Hakee kaikki `jaksot` listassa olevien jaksojen kanssa päällekäin olevat
  jaksot eHOKSista."
  [jaksot]
  (ehoks/get-tyoelamajaksot-active-between!
    (:oppija_oid (first jaksot))
    (apply s/first (map :jakso_alkupvm jaksot))
    (apply s/last  (map :jakso_loppupvm jaksot))))

(defn get-and-memoize-opiskeluoikeudet!
  "Funktio hakee `jaksot` listan jaksojen opiskeluoikeuksia Koskesta.
  Opiskeluoikeudet tallennetaan muistiin hakujen välillä, joten jos listassa on
  jaksoja jotka jakavat saman opiskeluoikeuden, näiden opiskeluoikeudet
  tarvitsee hakea Koskesta vain kerran. Näin vältetään turhia GET-pyyntöjä
  Koskeen."
  [jaksot]
  (reduce (fn [memoized-opiskeluoikeudet oid]
            (if (contains? memoized-opiskeluoikeudet oid)
              memoized-opiskeluoikeudet
              (conj memoized-opiskeluoikeudet
                    [oid (koski/get-opiskeluoikeus-catch-404! oid)])))
            {}
            (map :opiskeluoikeus_oid jaksot)))

(defn calculate-kestot!
  [jaksot]
  (apply
    merge-with
    #(vector (merge (first %) (first %2)) (merge (second %) (second %2)))
    (map (fn [oppijan-jaksot]
           (let [concurrent-jaksot (get-concurrent-jaksot-from-ehoks!
                                     oppijan-jaksot)
                 opiskeluoikeudet  (get-and-memoize-opiskeluoikeudet!
                                    concurrent-jaksot)]
            ; Tilapäinen: Työpaikkajaksojen kestot lasketaan sekä uudella
            ; että vanhalla laskentatavalla. Samanaikaiset jaksot ja
            ; opiskeluoikeudet haettu yllä, jotta niitä ei tarvitse hakea
            ; eHOKSista ja Koskesta kahteen kertaan.
            [(calculate-kestot     concurrent-jaksot opiskeluoikeudet)
             (calculate-kestot-old concurrent-jaksot opiskeluoikeudet)]))
         ; Ryhmitellään jaksot oppija-oid:n perusteella:
         (vals (group-by :oppija_oid jaksot)))))


(defn retrieve-and-update-jaksot!
  "Hakee nippuun kuuluvat jaksot tietokannasta, laskee niiden kestot, päivittää
  kestotiedot tietokantaan, ja palauttaa päivitetyt jaksot."
  [nippu]
  (let [jaksot (query-jaksot! nippu)
        [kestot kestot-old] (calculate-kestot! jaksot)]
    (map #(let [kesto     (get kestot     (:hankkimistapa_id %))
                kesto-old (get kestot-old (:hankkimistapa_id %))]
            (tc/update-jakso % {:kesto [:n kesto] :kesto-vanha [:n kesto-old]})
            (assoc % :kesto kesto :kesto-vanha kesto-old))
         jaksot)))

(defn niputa
  "Luo nippukyselylinkin jokaiselle alustavalle nipulle, jos sillä on vielä
  jaksoja, joilla on vielä aikaa vastata."
  [nippu]
  (log/info "Niputetaan " nippu)
  (let [request-id (c/generate-uuid)
        jaksot (retrieve-and-update-jaksot! nippu)
        tunnukset (vec (map (fn [x] {:tunnus                (:tunnus x)
                                     :tyopaikkajakson_kesto (:kesto x)})
                            jaksot))]
    (if (not-empty tunnukset)
      (let [tunniste (c/create-nipputunniste (:tyopaikan_nimi (first jaksot)))
            arvo-resp (arvo/create-nippu-kyselylinkki
                        (arvo/build-niputus-request-body tunniste
                                                         nippu
                                                         tunnukset
                                                         request-id))]
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
