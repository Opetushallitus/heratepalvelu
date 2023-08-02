(ns oph.heratepalvelu.tep.niputusHandler
  "Käsittelee alustavia nippuja ja luo niille kyselylinkeille."
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.util.date :as d]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.external.ehoks :as ehoks]
            [oph.heratepalvelu.external.koski :as koski]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.tep.tepCommon :as tc])
  (:import (java.lang Math)
           (java.time LocalDate)
           (software.amazon.awssdk.awscore.exception AwsServiceException)
           (software.amazon.awssdk.services.dynamodb.model
             ConditionalCheckFailedException)))

(gen-class
  :name "oph.heratepalvelu.tep.niputusHandler"
  :methods [[^:static handleNiputus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn round-vals [m] (reduce-kv #(assoc %1 %2 (Math/round ^Double %3)) {} m))

(defn not-in-keskeytymisajanjakso?
  "Varmistaa, että annettu päivämäärä ei kuulu keskeytymisajanjaksoon."
  [^LocalDate date keskeytymisajanjaksot]
  (or (empty? keskeytymisajanjaksot)
      (every? #(or (and (:alku %) (.isBefore date (:alku %)))
                   (and (:loppu %) (.isAfter date (:loppu %))))
              keskeytymisajanjaksot)))

(defn osa-aikaisuuskerroin
  "Hakee osa-aikaisuustiedon jaksosta ja varmistaa että se on validi (ts.
  kokonaisluku väliltä 1 - 100). Palauttaa osa-aikaisuuskertoimen, joka
  lasketaan jakamalla prosentuaalinen osa-aikaisuus 100 %:lla,
  esim. 80 % / 100 % = 0.8. Jos osa-aikaisuustieto puuttuu tai on ei-validi,
  palautetaan osa-aikaisuuskertoimena nolla."
  [jakso]
  (let [id            (:hankkimistapa_id jakso)
        osa-aikaisuus (:osa_aikaisuus jakso)]
    (or (cond
          (nil? osa-aikaisuus)
          (log/error "Osa-aikaisuustieto puuttuu jakson" id "tiedoista. Jakson"
                     "kestoksi asetetaan nolla.")
          (not (and (integer? osa-aikaisuus)
                    (pos? osa-aikaisuus)
                    (>= 100 osa-aikaisuus)))
          (log/error "Jakson" id "osa-aikaisuus" (str "`" osa-aikaisuus "`")
                     "ei ole validi. Jakson kestoksi asetetaan nolla.")
          :else (/ osa-aikaisuus 100.0))
        0)))

(defn add-loppu-to-jaksot
  "Lisää jokaiseen paitsi viimeiseen jaksoon kentän :loppu, joka on päivää
  ennen kuin seuraavan :alku"
  [jaksot]
  (conj (mapv (fn [current next]
                (let [^LocalDate next-starts (:alku next)]
                  (assoc current :loppu (.minusDays next-starts 1))))
              jaksot
              (rest jaksot))
        (last jaksot)))

(defn get-opiskeluoikeusjaksot
  [opiskeluoikeus]
  (->> (:opiskeluoikeusjaksot (:tila opiskeluoikeus))
       (map c/alku-and-loppu-to-localdate)
       (sort-by :alku)
       add-loppu-to-jaksot))

(defn keskeytymisajanjaksot
  [jakso opiskeluoikeus]
  (let [keskeytynyt-tila? #{"valiaikaisestikeskeytynyt"}
        kjaksot-in-jakso (map c/alku-and-loppu-to-localdate
                              (:keskeytymisajanjaksot jakso))
        kjaksot-in-opiskeluoikeus
        (filter #(keskeytynyt-tila? (:koodiarvo (:tila %)))
                (get-opiskeluoikeusjaksot opiskeluoikeus))]
    (concat kjaksot-in-jakso kjaksot-in-opiskeluoikeus)))

(defn in-jakso?
  "Tarkistaa sisältyykö päivämäärä `pvm` jaksoon `jakso`. Oletus on, että
  sekä pvm että jakson avaimet :alku ja :loppu ovat tyyppiä `LocalDate`.
  Palauttaa `true` jos päivämäärä sisältyy jaksoon, muuten `false`."
  [^LocalDate pvm jakso]
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
                 (keskeytymisajanjaksot jakso opiskeluoikeus))))

(defn oppijan-jaksojen-yhden-paivan-kestot
  "Laskee yhden oppijan aktiivisena olevien jaksojen kestot yhden päivän osalta,
  eli suorittaa niin sanotun 'jyvityksen'. Tällä tarkoitetaan sitä, että yhden
  päivän kesto jaetaan tasaisesti kaikille samanaikaisesti aktiivisena oleville
  (ei keskeytyneille) jaksoille kyseisen päivän osalta.

  Funktio olettaa, että parametrina saadut `jaksot` ovat kaikki saman oppijan
  jaksoja. `opiskeluoikeudet` hashmap (oid -> opiskeluoikeus) voi sisältää
  muidenkin oppijoiden opiskeluoikeuksia, sillä kutakin jaksoa vastaava
  opiskeluoikeus haetaan `opiskeluoikeudet` hashmapista käyttäen jakson
  tiedoista löytyvää opiskeluoikeuden oid:ta.

  Funktio palauttaa päivänä `pvm` aktiivisena olleiden jaksojen jyvittämällä
  lasketut kestot hashmapissa, jossa avaimina jaksojen id:t
  (osaamisenhankkimistapojen) ja arvoina lasketut kestot reaalilukuina.

  Esimerkki:
  `jaksot` listassa on 4 jaksoa id:illä 1, 2, 3 ja 4. Näistä 1, 2 ja 4 ovat
  aktiivisia päivänä `pvm` ja 3 on keskeytynyt. Tällöin funktio palauttaa
  {1 0.333... 2 0.333... 4 0.333...}. Keskeytyneen jakson nollakesto ei ole
  siis mukana palautettavassa hashmapissa."
  [jaksot opiskeluoikeudet pvm]
  (let [active-jakso-ids ; Päivänä `pvm` aktiivisena olevien jaksojen id:t
        (map :hankkimistapa_id
             (filter #(jakso-active? %
                                     (get opiskeluoikeudet
                                          (:opiskeluoikeus_oid %))
                                     pvm)
                     jaksot))
        num-of-active-jaksos (count active-jakso-ids)
        kesto (/ 1.0 num-of-active-jaksos)] ; Jyvitys
    (zipmap active-jakso-ids (repeat kesto))))

(defn harmonize-date-keys
  "Harmonisoi jakson alku- ja loppupäivämääriä vastaavat avaimet, jotta
  kestonlaskennassa voidaan hyödyntää mahdollisimman paljon samoja funktiota."
  [jakso]
  (cond-> jakso
    (:jakso_alkupvm jakso)  (assoc :alku (:jakso_alkupvm jakso))
    (:jakso_loppupvm jakso) (assoc :loppu (:jakso_loppupvm jakso))))

(defn harmonize-alku-and-loppu-dates
  "Harmonisoi :jakso_alkupvm ja :jakso_loppupvm avaimet avaimiksi
  :alku ja :loppu myöhempää prosessointia varten. Muuttaa myös vastaavat
  päivämäärät myös LocalDate-objekteiksi."
  [jakso]
  (c/alku-and-loppu-to-localdate (harmonize-date-keys jakso)))

(defn oppijan-jaksojen-kestot
  "Laskee kestot jaksoille `jaksot`. Funktio olettaa, että `jaksot` pitävät
  sisällään ainoastaan yhden oppijan jaksoja. `opiskeluoikeudet` tulee olla
  hashmap (oid -> opiskeluoikeus). Se voi sisältää muidenkin oppijoiden
  opiskeluoikeuksia, sillä jaksoja `jaksot` vastaavat opiskeluoikeudet haetaan
  `opiskeluoikeudet` hashmapista jakson tiedoista löytyvän opiskeluoikeus oid:n
  perusteella.

  Funktio palauttaa yhden oppijan jaksojen kestot aikavälillä, missä aikavälin
  alku on jaksojen `jaksot` alkupäivämääristä varhaisin ja loppu vastaavasi
  päättymispäivistä myöhäisin. Palautuva arvo on hashmap, jossa avaimina
  jaksojen id:t (osaamisenhankkimistapojen) ja arvoina kestot kokonaislukuina.
  Yksittäisen jakson kesto voi olla nolla, jos kyseiselle jaksolle ei saada
  opiskeluoikeutta Koskesta 404-virheen vuoksi, tai jos jakson
  osa-aikaisuustieto puuttuu tai on virheellinen. Jakson kesto voi myös
  pyöristyä nollaan, mikäli kokonaiskesto on jotain 0 ja 0.5 väliltä."
  [oppijan-jaksot opiskeluoikeudet]
  (when (not-empty oppijan-jaksot)
    (let [jaksot (map harmonize-alku-and-loppu-dates oppijan-jaksot)
          ids    (map :hankkimistapa_id jaksot)]
      (round-vals ; Pyöristetään kestot lähimpään kokonaislukuun.
        (merge-with
          * ; Kerrotaan kestot osa-aikaisuuskertoimilla
          (reduce (partial merge-with +) ; Summataan yksittäisten päivien kestot
                  ; Alustetaan alla kaikki kestot nollaksi:
                  (zipmap ids (repeat 0))
                  (map (partial oppijan-jaksojen-yhden-paivan-kestot
                                jaksot
                                opiskeluoikeudet)
                       (d/range (first (sort (map :alku jaksot)))
                                (last  (sort (map :loppu jaksot))))))
          (zipmap ids (map osa-aikaisuuskerroin jaksot)))))))

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
  saman oppijan jaksot eHOKSista."
  [jaksot]
  (ehoks/get-tyoelamajaksot-active-between!
    (:oppija_oid (first jaksot))
    (first (sort (map :jakso_alkupvm jaksot)))
    (last  (sort (map :jakso_loppupvm jaksot)))))

(defn get-and-memoize-opiskeluoikeudet!
  "Funktio hakee `jaksot` listan jaksojen opiskeluoikeuksia Koskesta.
  Opiskeluoikeudet tallennetaan muistiin hakujen välillä, joten jos listassa on
  jaksoja jotka jakavat saman opiskeluoikeuden, näiden opiskeluoikeudet
  tarvitsee hakea Koskesta vain kerran. Näin vältetään turhia GET-pyyntöjä
  Koskeen."
  [jaksot]
  (reduce
    (fn [memoized-opiskeluoikeudet jakso]
      (let [oht-id (:hankkimistapa_id jakso)
            oo-oid (:opiskeluoikeus_oid jakso)]
        (if (contains? memoized-opiskeluoikeudet oo-oid)
          memoized-opiskeluoikeudet
          (if-let [opiskeluoikeus (koski/get-opiskeluoikeus-catch-404! oo-oid)]
            (conj memoized-opiskeluoikeudet [oo-oid opiskeluoikeus])
            (do (log/warn "Opiskeluoikeutta" (str "`" oo-oid "`") "ei saatu"
                          "Koskesta. Jakson" oht-id "kestoksi asetetaan nolla.")
                memoized-opiskeluoikeudet)))))
    {}
    jaksot))

(defn jaksojen-kestot!
  "Laskee kestot kaikille `jaksot` listan jaksoille. Lista jaksoista `jaksot`
  voi pitää sisällään useamman oppijan työpaikkajaksoja: Kestonlaskenta tehdään
  varsinaisesti aina saman oppijan jaksoille (kts. `oppijan-jaksojen-kestot`),
  mutta tässä funktiossa `jaksot` ryhmitellään oppija oid:n mukaan ennen
  `oppijan-jaksojen-kestot`-funktion tekemää kestojen laskemista.

  Palauttaa hashmapin, joka sisältää `jaksot` listan jaksoille lasketut kestot.
  Hashmapin avaimina jaksojen id:t (osaamisenhankkimistapojen) ja arvoina kestot
  kokonaislukuina."
  [jaksot]
  (->> (group-by :oppija_oid jaksot) ; Ryhmitellään jaksot oppijan perusteella
       vals
       ; Haetaan kunkin jakson tapauksessa päällekäiset jaksot eHOKSista
       ; sekä viimeisin opiskeluoikeustieto Koskesta:
       (map (fn [oppijan-jaksot]
              (let [concurrent-jaksot (get-concurrent-jaksot-from-ehoks!
                                        oppijan-jaksot)
                    opiskeluoikeudet  (get-and-memoize-opiskeluoikeudet!
                                        concurrent-jaksot)]
                (oppijan-jaksojen-kestot concurrent-jaksot opiskeluoikeudet))))
       ; Yhdistetään eri oppijoiden jaksoille lasketut kestot yhdeksi
       ; hashmapiksi:
       (apply merge)
       ; Palautetaan vain niiden jaksojen kestot, jotka ovat `jaksot` listassa:
       (#(select-keys % (map :hankkimistapa_id jaksot)))))

(defn retrieve-and-update-jaksot!
  "Hakee nippuun kuuluvat jaksot tietokannasta, laskee niiden kestot, päivittää
  kestotiedot tietokantaan, ja palauttaa päivitetyt jaksot."
  [nippu]
  (let [jaksot (query-jaksot! nippu)
        kestot (jaksojen-kestot! jaksot)]
    (map #(let [oht-id (:hankkimistapa_id %)
                kesto  (get kestot oht-id 0)]
            (log/info "Päivitetään jaksoon" oht-id "kesto" kesto)
            (tc/update-jakso % {:kesto [:n kesto]})
            (assoc % :kesto kesto))
         jaksot)))

(defn niputa
  "Luo nippukyselylinkin jokaiselle alustavalle nipulle, jos sillä on vielä
  jaksoja, joilla on vielä aikaa vastata."
  [nippu]
  (log/info "Niputetaan" nippu)
  (let [request-id (c/generate-uuid)
        jaksot (retrieve-and-update-jaksot! nippu)
        tunnukset (vec (map (fn [x] {:tunnus                (:tunnus x)
                                     :tyopaikkajakson_kesto (:kesto x)})
                            jaksot))]
    (if (not-empty tunnukset)
      (let [tunniste (c/create-nipputunniste (:tyopaikan_nimi (first jaksot)))
            arvo-req (arvo/build-niputus-request-body
                       tunniste nippu tunnukset request-id)
            arvo-resp (arvo/create-nippu-kyselylinkki arvo-req)]
        (log/info "Luotu kyselylinkki pyynnöllä" arvo-req
                  "; arvon vastaus" arvo-resp)
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
            (catch ConditionalCheckFailedException _
              (log/warn "Nipulla"
                        (:ohjaaja_ytunnus_kj_tutkinto nippu)
                        "on jo kantaan tallennettu kyselylinkki.")
              (arvo/delete-nippukyselylinkki tunniste))
            (catch AwsServiceException e
              (log/error "Virhe DynamoDB tallennuksessa " e)
              (arvo/delete-nippukyselylinkki tunniste)
              (throw e)))
          (do (log/error "Virhe niputuksessa" nippu request-id)
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
                   {:index "niputusIndex"}
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
  (let [processed-niput (atom {})
        timeout? (c/no-time-left? context 60000)
        niputettavat (sort-by :niputuspvm #(- (compare %1 %2)) (do-query))]
    (log/info "Aiotaan käsitellä" (count niputettavat) "niputusta.")
    (when (seq niputettavat)
      (c/doseq-with-timeout
        timeout?
        [nippu niputettavat]
        (if (get @processed-niput (get-nippu-key nippu))
          (log/warn "Nippu on jo käsitelty" nippu)
          (try (niputa nippu)
               (swap! processed-niput assoc (get-nippu-key nippu) true)
               (catch Exception e
                 (log/error e "nipussa" nippu))))))))
