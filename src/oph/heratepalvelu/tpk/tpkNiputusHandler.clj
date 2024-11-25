(ns oph.heratepalvelu.tpk.tpkNiputusHandler
  "Käsittelee TPK:n niputusta ja tallennusta herätepalvelun tietokantaan."
  (:require [clojure.core.memoize :refer [memo]]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.tpk.tpkCommon :as tpkc]
            [oph.heratepalvelu.util.string :as u-str])
  (:import (java.time LocalDate)
           (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class :name "oph.heratepalvelu.tpk.tpkNiputusHandler"
           :methods
           [[^:static handleTpkNiputus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn jakso-valid-for-tpk?
  "Varmistaa, että jaksossa on kaikki pakolliset kentät ja oppisopimuksen
  perusta ei ole 02 (yrittäjä)."
  [jakso]
  (and (:koulutustoimija jakso)
       (:tyopaikan_nimi jakso)
       (:tyopaikan_ytunnus jakso)
       (:jakso_loppupvm jakso)
       (:tunnus jakso)
       (or (not= (:hankkimistapa_tyyppi jakso) "oppisopimus")
           (not= (:oppisopimuksen_perusta jakso) "02"))))

(defn create-nippu-id
  "Luo nipputunnisteen työpaikan normalisoidun nimen, työpaikan Y-tunnuksen,
  koulutustoimijan ja tiedonkeruukauden perustella."
  [jakso]
  (str (u-str/normalize (:tyopaikan_nimi jakso)) "/"
       (:tyopaikan_ytunnus jakso) "/"
       (:koulutustoimija jakso) "/"
       (tpkc/get-kausi-alkupvm (LocalDate/parse (:jakso_loppupvm jakso))) "_"
       (tpkc/get-kausi-loppupvm (LocalDate/parse (:jakso_loppupvm jakso)))))

(defn get-existing-nippu
  "Hakee nipun tietokannasta jakson tietojen perusteella, jos se on olemassa."
  [jakso]
  (try
    (ddb/get-item {:nippu-id [:s (create-nippu-id jakso)]}
                  (:tpk-nippu-table env))
    (catch AwsServiceException e
      (log/error "Haku DynamoDB:stä epäonnistunut:" e)
      (throw e))))

(defn get-next-vastaamisajan-alkupvm-date
  "Laske vastaamisajan alkupäivämäärän jakso loppupäivämäärän perusteella."
  ^LocalDate [jakso]
  (let [loppupvm (LocalDate/parse (:jakso_loppupvm jakso))
        year (.getYear loppupvm)
        kausi-month (if (<= (.getMonthValue loppupvm) 6) 7 1)
        kausi-year (if (= kausi-month 1) (inc year) year)]
    (LocalDate/of ^long kausi-year kausi-month 1)))

(defn create-tpk-nippu
  "Luo TPK-nipun jakson tietojen perusteella."
  [jakso]
  (let [alkupvm (get-next-vastaamisajan-alkupvm-date jakso)
        loppupvm (.minusDays (.plusMonths alkupvm 2) 1)
        kausi-alkupvm (tpkc/get-kausi-alkupvm
                        (LocalDate/parse (:jakso_loppupvm jakso)))
        kausi-loppupvm (tpkc/get-kausi-loppupvm
                         (LocalDate/parse (:jakso_loppupvm jakso)))]
    {:nippu-id                    (create-nippu-id jakso)
     :tyopaikan-nimi              (:tyopaikan_nimi jakso)
     :tyopaikan-nimi-normalisoitu (u-str/normalize (:tyopaikan_nimi jakso))
     :vastaamisajan-alkupvm       (str alkupvm)
     :vastaamisajan-loppupvm      (str loppupvm)
     :tyopaikan-ytunnus           (:tyopaikan_ytunnus jakso)
     :koulutustoimija-oid         (:koulutustoimija jakso)
     :tiedonkeruu-alkupvm         (str kausi-alkupvm)
     :tiedonkeruu-loppupvm        (str kausi-loppupvm)
     :niputuspvm                  (str (c/local-date-now))}))

(defn save-tpk-nippu
  "Tallentaa nipun tietokantaan."
  [nippu]
  (try
    (ddb/put-item
      ; FIXME: map-values
      (reduce #(assoc %1 (first %2) [:s (second %2)]) {} (seq nippu))
      {}
      (:tpk-nippu-table env))
    (catch AwsServiceException e
      (log/error "Virhe DynamoDB tallennuksessa (TPK):" e))))

(defn update-tpk-niputuspvm
  "Päivittää jakson TPK-niputuspäivämäärän jaksotunnus-tauluun."
  [jakso new-value]
  (ddb/update-item
    {:hankkimistapa_id [:n (:hankkimistapa_id jakso)]}
    {:update-expr "SET #value = :value"
     :expr-attr-names {"#value" "tpk-niputuspvm"}
     :expr-attr-vals {":value" [:s new-value]}}
    (:jaksotunnus-table env)))

(defn query-niputtamattomat
  "Hakee jaksoja ko. tiedonkeruukauden alkupäivästä tiedonkeruukauden
  loppupäivään asti (tai nykyiseen päivään asti, jos tiedonkeruukauden
  loppupäivä ei ole vielä tullut). Hakee vain ne jaksot, joiden
  TPK-niputuspäivämäärä ei ole vielä määritelty.

  Jos edeltävän kauden vastausaika ei ole loppunut, niputtaa jaksoja edeltävästä
  kaudesta. Muuten niputtaa seuraavan kauden jaksot."
  [exclusive-start-key]
  (let [today (c/local-date-now)
        start-date (str (tpkc/get-current-kausi-alkupvm))
        kausi-end-date (tpkc/get-current-kausi-loppupvm)
        end-date (if (.isAfter today kausi-end-date)
                   (str kausi-end-date)
                   (str today))]
    (ddb/scan {:filter-expression
               "#tpkNpvm = :tpkNpvm AND #jl BETWEEN :start AND :end"
               :exclusive-start-key exclusive-start-key
               :expr-attr-names {"#tpkNpvm" "tpk-niputuspvm"
                                 "#jl"      "jakso_loppupvm"}
               :expr-attr-vals {":tpkNpvm" [:s "ei_maaritelty"]
                                ":end"     [:s end-date]
                                ":start"   [:s start-date]}}
              (:jaksotunnus-table env))))

(def ensure-nippu!
  "Huolehtii, että jaksolle on nippu tietokannassa, ja palauttaa sen."
  (memo
    ^{:clojure.core.memoize/args-fn (partial mapv create-nippu-id)}
    (fn [jakso]
      (or (not-empty (get-existing-nippu jakso))
          (let [nippu (create-tpk-nippu jakso)]
            (log/info "Luodaan uusi nippu tietokantaan:" nippu)
            (save-tpk-nippu nippu)
            nippu)))))

(defn handle-jakso!
  "Luo tpk-nipun yhdestä työpaikkajaksosta tai lisää jakson olemassa olevaan."
  [jakso]
  (log/info "Käsitellään jakso" jakso)
  (update-tpk-niputuspvm
    jakso
    (if (jakso-valid-for-tpk? jakso)
      (let [nippu (ensure-nippu! jakso)] (:niputuspvm nippu))
      "ei_niputeta")))

(defn -handleTpkNiputus
  "Käsittelee työpaikkajaksoja ja luo vastaavia TPK-nippuja. Yhteen nippuun voi
  kuulua useita jaksoja."
  [_ event ^com.amazonaws.services.lambda.runtime.Context context]
  (log-caller-details-scheduled "handleTpkNiputus" event context)
  (loop [niputettavat (query-niputtamattomat nil)]
    (let [jaksot (:items niputettavat)]
      (log/info "Aiotaan käsitellä" (count jaksot) "työpaikkajaksoa.")
      (doseq [jakso jaksot]
        (try (handle-jakso! jakso)
             (catch Exception e
               (log/error e "jaksossa" jakso))))
      (when (and (< 30000 (.getRemainingTimeInMillis context))
                 (:last-evaluated-key niputettavat))
        (recur (query-niputtamattomat (:last-evaluated-key niputettavat)))))))
