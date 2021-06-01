(ns oph.heratepalvelu.tep.niputusHandler
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.arvo :as arvo]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.common :as c]
            [clojure.tools.logging :as log]
            [clj-time.core :as t]
            [environ.core :refer [env]]
            [clojure.string :as str]
            [clj-http.util :as util])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)
           (clojure.lang ExceptionInfo)
           (software.amazon.awssdk.services.dynamodb.model ConditionalCheckFailedException)))

(gen-class
  :name "oph.heratepalvelu.tep.niputusHandler"
  :methods [[^:static handleNiputus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn- niputa [nippu]
  (log/info "Niputetaan " nippu)
  (let [request-id (c/generate-uuid)
        jaksot (filter
                 #(>= (compare (:viimeinen_vastauspvm %1)
                               (str (t/today)))
                      0)
                 (ddb/query-items {:ohjaaja_ytunnus_kj_tutkinto [:eq [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]]
                                   :niputuspvm                  [:eq [:s (:niputuspvm nippu)]]}
                                  {:index "niputusIndex"
                                   :filter-expression "#pvm >= :pvm"
                                   :expr-attr-names {"#pvm" "viimeinen_vastauspvm"}
                                   :expr-attr-vals {":pvm" [:s (str (t/today))]}}
                                  (:jaksotunnus-table env)))
        tunnukset (map :tunnus jaksot)]
    (if (not-empty tunnukset)
      (let [tunniste (str
                       (str/replace (:tyopaikan_nimi (first jaksot))
                                    #"[\\|/|\?|#|\s]" "_")
                       "_" (t/today) "_" (c/rand-str 6))
            arvo-resp (arvo/create-nippu-kyselylinkki
                        (arvo/build-niputus-request-body
                          tunniste
                          (if (some? (:tyopaikka nippu))
                            nippu
                            (assoc nippu :tyopaikka (:tyopaikan-nimi (first jaksot)))) ;poista ennen tuotantoa
                          tunnukset
                          request-id))]
        (if (some? (:nippulinkki arvo-resp))
          (try
            (ddb/update-item
              {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]
               :niputuspvm                  [:s (:niputuspvm nippu)]}
              {:update-expr     (str "SET #tila = :tila, "
                                     "#pvm = :pvm, "
                                     "#linkki = :linkki, "
                                     "#voimassa = :voimassa, "
                                     "#req = :req")
               :cond-expr (str "attribute_not_exists(kyselylinkki)")
               :expr-attr-names {"#tila" "kasittelytila"
                                 "#linkki" "kyselylinkki"
                                 "#voimassa" "voimassaloppupvm"
                                 "#req" "request_id"
                                 "#pvm" "kasittelypvm"}
               :expr-attr-vals {":tila"     [:s (:ei-lahetetty c/kasittelytilat)]
                                ":linkki"   [:s (:nippulinkki arvo-resp)]
                                ":voimassa" [:s (:voimassa_loppupvm arvo-resp)]
                                ":req"      [:s request-id]
                                ":pvm"      [:s (str (t/today))]}}
              (:nippu-table env))
            (catch ConditionalCheckFailedException e
              (log/warn "Nipulla " (:ohjaaja_ytunnus_kj_tutkinto nippu) " on jo kantaan tallennettu kyselylinkki.")
              (arvo/delete-nippukyselylinkki tunniste))
            (catch AwsServiceException e
              (log/error "Virhe DynamoDB tallennuksessa " e)
              (arvo/delete-nippukyselylinkki tunniste)
              (throw e)))
          (do (log/error "Virhe niputuksessa " nippu request-id)
              (ddb/update-item
                {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]
                 :niputuspvm                  [:s (:niputuspvm nippu)]}
                {:update-expr     (str "SET #tila = :tila, "
                                       "#pvm = :pvm, "
                                       "#reason = :reason, "
                                       "#req = :req")
                 :expr-attr-names {"#tila" "kasittelytila"
                                   "#reason" "reason"
                                   "#req" "request_id"
                                   "#pvm" "kasittelypvm"}
                 :expr-attr-vals {":tila"     [:s "niputusvirhe"]
                                  ":reason"   [:s (str (or (:errors arvo-resp) "no reason in response"))]
                                  ":req"      [:s request-id]
                                  ":pvm"      [:s (str (t/today))]}}
                (:nippu-table env)))))
      (do (log/warn "Ei jaksoja, joissa vastausaikaa jäljellä " (:ohjaaja_ytunnus_kj_tutkinto nippu) (:niputuspvm nippu))
          (ddb/update-item
            {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]
             :niputuspvm                  [:s (:niputuspvm nippu)]}
            {:update-expr     (str "SET #tila = :tila, "
                                   "#pvm = :pvm, "
                                   "#reason = :reason, "
                                   "#req = :req")
             :expr-attr-names {"#tila" "kasittelytila"
                               "#req" "request_id"
                               "#pvm" "kasittelypvm"}
             :expr-attr-vals {":tila"     [:s "ei-jaksoja"]
                              ":req"      [:s request-id]
                              ":pvm"      [:s (str (t/today))]}}
            (:nippu-table env))))))

(defn -handleNiputus [this event context]
  (log-caller-details-scheduled "handleNiputus" event context)
  (loop [niputettavat
         (sort-by
           :niputuspvm
           #(* -1 (compare %1 %2))
           (ddb/query-items {:kasittelytila [:eq [:s (:ei-niputettu c/kasittelytilat)]]
                             :niputuspvm    [:le [:s (str (t/today))]]}
                            {:index "niputusIndex"
                             :limit 10}
                            (:nippu-table env)))]
    (log/info "Käsitellään " (count niputettavat) " niputusta.")
    (when (seq niputettavat)
      (doseq [nippu niputettavat]
        (niputa nippu))
      (when (< 120000 (.getRemainingTimeInMillis context))
        (recur (ddb/query-items {:kasittelytila [:eq [:s (:ei-niputettu c/kasittelytilat)]]
                                 :niputuspvm    [:le [:s (str (t/today))]]}
                                {:index "niputusIndex"
                                 :limit 10}
                                (:nippu-table env)))))))
