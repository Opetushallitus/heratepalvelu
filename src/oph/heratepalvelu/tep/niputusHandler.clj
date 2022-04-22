(ns oph.heratepalvelu.tep.niputusHandler
  "Käsittelee alustavia nippuja ja luo niille kyselylinkeille."
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.tep.tepCommon :as tc])
  (:import (clojure.lang ExceptionInfo)
           (com.amazonaws.services.lambda.runtime Context)
           (java.time LocalDate)
           (java.time.temporal ChronoUnit)
           (software.amazon.awssdk.awscore.exception AwsServiceException)
           (software.amazon.awssdk.services.dynamodb.model
             ConditionalCheckFailedException)))

(gen-class
  :name "oph.heratepalvelu.tep.niputusHandler"
  :methods [[^:static handleNiputus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn period-as-list-of-days
  ""
  [start end]
  (let [start-date (LocalDate/parse start)]
    (map #(.plusDays start-date %)
         (range (+ 1 (.between ChronoUnit/DAYS
                               start-date
                               (LocalDate/parse end)))))))

;; TODO täytyy sitten suodattaa tämä lista ja saada viikonloppupäivät ja
;; pyhäpäivät poistettua.

(defn filtered-jakso-days
  ""
  [jakso]
  ;; TODO filtering
  ;; TODO suodatetaanko myös keskeytyneet päivät pois tässä?
  (period-as-list-of-days (:jakso_alkupvm jakso) (:jakso_loppupvm jakso)))

;; TODO otetaanko me pyhäpäivät huomioon?

(defn add-to-jaksot-by-day
  ""
  [jaksot-by-day jakso]
  (reduce #(assoc %1 %2 (cons jakso (get %1 %2)))
          jaksot-by-day
          (filtered-jakso-days jakso)))

(defn create-all-jaksot-by-day
  ""
  [jaksot]
  (reduce add-to-jaksot-by-day {} jaksot))

;; TODO keskeytymisajanjaksot! Miten ne otetaan huomioon?
;; TODO käy päivämäärät läpi

(defn handle-one-day
  ""
  [jaksot]
  ;; TODO distribute the day across the different jaksot; taking osa-aikaisuus into account
  )

(defn merge-maps
  ""
  [maps]
  (reduce (fn [acc m] (reduce-kv #(assoc %1 %2 (+ %3 (get %1 %2 0))) acc m))
          {}
          maps))

(defn process-jaksot-by-day
  ""
  [jaksot-by-day]
  (merge-maps (map handle-one-day jaksot-by-day)))

;; TODO convert doubles to longs?

;; TODO extract those values that we actually want

(defn do-kesto-computation
  ""
  [jaksot]
  ;; TODO create-jaksot-by-day, process-jaksot-by-day, and get only those values we want
  )

(defn compute-kesto
  ""
  [jaksot]
  (let [first-start-date  (first (sort (map :jakso_alkupvm jaksot)))
        last-end-date     (first (reverse (sort (map :jakso_loppupvm jaksot))))
        concurrent-jaksot (ehoks/get-tyoelamajaksot-active-between
                            (:oppija_oid (first jaksot))
                            first-start-date
                            last-end-date)
        ;; TODO varmistaa, että olemassa olevat jaksotkin otetaan mukaan?
        ]
    (do-kesto-computation concurrent-jaksot)))

(defn niputa
  "Luo nippukyselylinkin jokaiselle alustavalle nipulle, jos sillä on vielä
  jaksoja, joilla on vielä aikaa vastata."
  [nippu]
  (log/info "Niputetaan " nippu)
  (let [request-id (c/generate-uuid)
        jaksot (filter
                 #(>= (compare (:viimeinen_vastauspvm %1)
                               (str (c/local-date-now)))
                      0)
                 (ddb/query-items
                   {:ohjaaja_ytunnus_kj_tutkinto
                    [:eq [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]]
                    :niputuspvm [:eq [:s (:niputuspvm nippu)]]}
                   {:index "niputusIndex"
                    :filter-expression (str "#pvm >= :pvm "
                                            "AND attribute_exists(#tunnus)")
                    :expr-attr-names {"#pvm"    "viimeinen_vastauspvm"
                                      "#tunnus" "tunnus"}
                    :expr-attr-vals {":pvm" [:s (str (c/local-date-now))]}}
                   (:jaksotunnus-table env)))
        tunnukset (map :tunnus jaksot)]
    (if (not-empty tunnukset)
      (let [tunniste (c/create-nipputunniste (:tyopaikan_nimi (first jaksot)))
            arvo-resp (arvo/create-nippu-kyselylinkki
                        (arvo/build-niputus-request-body
                          tunniste
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

(defn -handleNiputus
  "Hakee ja niputtaa niputtamattomat jaksot."
  [_ event ^com.amazonaws.services.lambda.runtime.Context context]
  (log-caller-details-scheduled "handleNiputus" event context)
  (loop [niputettavat
         (sort-by
           :niputuspvm
           #(* -1 (compare %1 %2))
           (do-query))]
    (log/info "Käsitellään" (count niputettavat) "niputusta.")
    (when (seq niputettavat)
      (doseq [nippu niputettavat]
        (niputa nippu))
      (when (< 120000 (.getRemainingTimeInMillis context))
        (recur (do-query))))))
