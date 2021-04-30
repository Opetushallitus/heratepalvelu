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
        prev (ddb/query-items
               {:ohjaaja_ytunnus_kj_tutkinto [:eq [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]]
                :niputuspvm [:lt [:s (:niputuspvm nippu)]]}
               {:filter-expression (str "kasittelytila = " (:ei-niputettu c/kasittelytilat))}
               (:nippu-table env))
        niput (filter #(< 0 (compare (:niputuspvm %1)
                                      (str (t/minus (t/today) (t/days 60)))))
                      (conj prev nippu))
        jaksot (flatten
                 (map
                   (fn [n]
                     (ddb/query-items {:ohjaaja_ytunnus_kj_tutkinto [:eq [:s (:ohjaaja_ytunnus_kj_tutkinto n)]]
                                       :niputuspvm                  [:eq [:s (:niputuspvm n)]]}
                                      {:index "niputusIndex"}
                                      (:jaksotunnus-table env)))
                   niput))
        tunnukset (map :tunnus jaksot)]
    (let [tunniste (util/url-encode
                     (str
                       (str/join (str/split (:tyopaikan_nimi (first jaksot)) #"\s"))
                       "_" (t/today) "_" (c/rand-str 6)))
          arvo-resp (arvo/create-nippu-kyselylinkki
                      (arvo/build-niputus-request-body
                        tunniste
                        (assoc nippu :tyopaikka (:tyopaikan_nimi (first jaksot)))
                        (sequence tunnukset)
                        request-id))]
      (if (some? (:nippulinkki arvo-resp))
        (try
          (ddb/update-item
            {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]
             :niputuspvm                  [:s (:niputuspvm nippu)]}
            {:update-expr     (str "SET #tila = :tila, "
                                   "#linkki = :linkki, "
                                   "#voimassa = :voimassa, "
                                   "#req = :req")
             :cond-expr (str "attribute_not_exists(linkki)")
             :expr-attr-names {"#tila" "kasittelytila"
                               "#linkki" "kyselylinkki"
                               "#voimassa" "voimassaloppupvm"
                               "#req" "request_id"}
             :expr-attr-vals {":tila"     [:s (:ei-lahetetty c/kasittelytilat)]
                              ":linkki"   [:s (:nippulinkki arvo-resp)]
                              ":voimassa" [:s (:voimassa_loppupvm arvo-resp)]
                              ":req"      [:s request-id]}}
            (:nippu-table env))
          (catch ConditionalCheckFailedException e
            (log/warn "Nipulla " (:ohjaaja_ytunnus_kj_tutkinto nippu) " on jo kantaan tallennettu kyselylinkki.")
            (arvo/delete-nippukyselylinkki tunniste))
          (catch AwsServiceException e
            (log/error "Virhe DynamoDB tallennuksessa " e)
            (arvo/delete-nippukyselylinkki tunniste)
            (throw e)))
        (log/error "Ei tunnusta " nippu request-id))
      (doseq [n prev]
        (try
          (ddb/update-item
            {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto n)]
             :niputuspvm                  [:s (:niputuspvm n)]}
            {:update-expr     (str "SET #tila = :tila, "
                                   "#sisaltyy = :sisaltyy")
             :expr-attr-names {"#tila" "kasittelytila"
                               "#sisaltyy" "sisaltyy"}
             :expr-attr-vals {":tila"     [:s (:yhdistetty c/kasittelytilat)]
                              ":sisaltyy" [:s (str (:ohjaaja_ytunnus_kj_tutkinto nippu) "/"
                                                   (:niputuspvm nippu))]}}
            (:nippu-table env))
          (catch AwsServiceException e
            (log/error "Virhe yhdistettyjen nippujen merkitsemisessä ("
                       (:ohjaaja_ytunnus_kj_tutkinto n) "/" (:niputuspvm n) ")")
            (log/error e)))))))

(defn -handleNiputus [this event context]
  (log-caller-details-scheduled "handleNiputus" event context)
  (loop [niputettavat
         (sort-by
           :niputuspvm
           #(* -1 (compare %1 %2))
           (ddb/query-items {:kasittelytila [:eq [:s (:ei-niputettu c/kasittelytilat)]]
                             :niputuspvm    [:le [:s (.toString (t/today))]]}
                            {:index "niputusIndex"
                             :limit 10}
                            (:nippu-table env)))]
    (log/info "Käsitellään " (count niputettavat) " niputusta.")
    (when (seq niputettavat)
      (doseq [nippu niputettavat]
        (niputa nippu))
      (when (< 120000 (.getRemainingTimeInMillis context))
        (recur (ddb/query-items {:kasittelytila [:eq [:s (:ei-niputettu c/kasittelytilat)]]
                                 :niputuspvm    [:le [:s (.toString (t/today))]]}
                                {:index "niputusIndex"
                                 :limit 10}
                                (:nippu-table env)))))))
