(ns oph.heratepalvelu.tpk.tpkArvoCallHandler
  "Hakee työpaikkakyselynipuille kyselylinkkejä Arvosta."
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.tpk.tpkCommon :as tpkc])
  (:import (clojure.lang ExceptionInfo)
           (com.amazonaws.services.lambda.runtime Context)
           (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class :name "oph.heratepalvelu.tpk.tpkArvoCallHandler"
           :methods
           [[^:static handleTpkArvoCalls
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn do-scan
  "Hakee TPK-nippuja tietokannasta, joissa ei ole vielä kyselylinkkiä ja jotka
  tiedonkeruukauden alkupäivämäärän perusteella kuuluvat tähän kauteen.

  Jos edeltävän kauden vastausaika ei ole loppunut, käsittelee edeltävän kauden
  niput. Muuten käsittelee seuraavan kauden niput."
  ([] (do-scan nil))
  ([exclusive-start-key]
   (ddb/scan {:filter-expression
              "attribute_not_exists(#linkki) AND #kausi = :kausi"
              :exclusive-start-key exclusive-start-key
              :expr-attr-names {"#kausi"  "tiedonkeruu-alkupvm"
                                "#linkki" "kyselylinkki"}
              :expr-attr-vals
              {":kausi" [:s (str (tpkc/get-current-kausi-alkupvm))]}}
             (:tpk-nippu-table env))))

(defn make-arvo-request
  "Pyytää TPK-kyselylinkin Arvosta."
  [nippu request-id]
  (try
    (arvo/create-tpk-kyselylinkki
      (arvo/build-tpk-request-body (assoc nippu :request-id request-id)))
    (catch ExceptionInfo e
      (log/error e "Ei luonut kyselylinkkiä nipulle:" nippu))))

(defn update-kyselylinkki-in-nippu!
  "Päivittää nipun kyselylinkin Arvon API-vastauksesta."
  [nippu arvo-resp request-id]
  (log/info "Päivitetään nippuun" (:nippu-id nippu)
            "kyselylinkki" (:kysely_linkki arvo-resp))
  (try
    (ddb/update-item
      {:nippu-id            [:s (:nippu-id nippu)]}
      {:update-expr (str "SET #linkki = :linkki, #tunnus = :tunnus, "
                         "#pvm = :pvm, #req = :req")
       :expr-attr-names {"#linkki" "kyselylinkki"
                         "#tunnus" "tunnus"
                         "#pvm"    "voimassa-loppupvm"
                         "#req"    "request-id"}
       :expr-attr-vals {":linkki" [:s (:kysely_linkki arvo-resp)]
                        ":tunnus" [:s (:tunnus arvo-resp)]
                        ":pvm"    [:s (:voimassa_loppupvm arvo-resp)]
                        ":req"    [:s request-id]}}
      (:tpk-nippu-table env))
    (catch AwsServiceException e
      (log/error e "päivitettäessä Arvo-vastausta" arvo-resp))))

(defn handle-single-nippu!
  "Luo kyselylinkin yhdelle TPK-nipulle."
  [nippu]
  (log/info "Käsitellään nippu" (:nippu-id nippu))
  (let [request-id (c/generate-uuid)
        arvo-resp (make-arvo-request nippu request-id)]
    (if (some? (:kysely_linkki arvo-resp))
      (update-kyselylinkki-in-nippu! nippu arvo-resp request-id)
      (log/error "Kyselylinkkiä ei saatu Arvolta. Nippu:" (:nippu-id nippu)
                 "Request ID:" request-id))))

(defn -handleTpkArvoCalls
  "Hakee TPK-nipuille kyselylinkkejä Arvosta. Luo vain yhden kyselylinkin per
  nippu, vaikka siihen kuuluisi useita jaksoja."
  [_ event ^Context context]
  (log-caller-details-scheduled "handleTpkArvoCalls" event context)
  (loop [scan-results (do-scan)]
    (log/info "Aiotaan käsitellä" (count (:items scan-results)) "nippua.")
    (doseq [nippu (:items scan-results)]
      (when (< 30000 (.getRemainingTimeInMillis context))
        (try (handle-single-nippu! nippu)
             (catch Exception e
               (log/error e "nipussa" nippu)))))
    (when (and (< 30000 (.getRemainingTimeInMillis context))
               (:last-evaluated-key scan-results))
      (recur (do-scan (:last-evaluated-key scan-results))))))
