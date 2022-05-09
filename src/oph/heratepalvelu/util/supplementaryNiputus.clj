(ns oph.heratepalvelu.util.supplementaryNiputus
  "Luo niput niille jaksoille, joiden niput jäivät muodostumatta 2022-05-06."
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb])
  (:import (software.amazon.awssdk.services.dynamodb DynamoDbClient)
           (software.amazon.awssdk.services.dynamodb.model GetItemRequest)
           (software.amazon.awssdk.regions Region)
           (software.amazon.awssdk.core.client.config
             ClientOverrideConfiguration)
           (com.amazonaws.xray.interceptors TracingInterceptor)))

(gen-class
  :name "oph.heratepalvelu.util.supplementaryNiputus"
  :methods [[^:static handleSupplementaryNiputus
             [com.amazonaws.services.lambda.runtime.events.SQSEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(def niput-updated (atom 0))

(def ^DynamoDbClient ddb-client
  "DynamoDB client -objekti."
  (-> (DynamoDbClient/builder)
      (.region (Region/EU_WEST_1))
      (.overrideConfiguration
        (-> (ClientOverrideConfiguration/builder)
            (.addExecutionInterceptor (TracingInterceptor.))
            ^ClientOverrideConfiguration (.build)))
      (.build)))

(defn consistent-get-item
  "Hakee yhden tietueen tietokannasta key-condsin perusteella. Avaimien syntaksi
  seuraa tätä mallia:
    {:<key> [:<tyyppi> <arvo>]}"
  [key-conds table]
  (let [req (-> (GetItemRequest/builder)
                (.tableName table)
                (.key (ddb/map-vals-to-attribute-values key-conds))
                (.consistentRead true)
                ^GetItemRequest (.build))
        response (.getItem ddb-client req)
        item (.item response)]
    (ddb/map-attribute-values-to-vals item)))

(defn conditional-create-nippu
  "Luo nipun, jos sitä ei ole tai jos olemassaolevassa nipussa on käsittelytila
  ei-niputeta."
  [jakso]
  (let [oykt (str (:ohjaaja_nimi jakso) "/" (:tyopaikan_ytunnus jakso) "/"
                  (:koulutustoimija jakso) "/" (:tutkinto jakso))
        niputuspvm (:niputuspvm jakso)
        ;; On pakko käyttää consistent read; muuten voi hakea vanhaa tietoa.
        existing-nippu (consistent-get-item
                         {:ohjaaja_ytunnus_kj_tutkinto [:s oykt]
                          :niputuspvm                  [:s niputuspvm]}
                         (:nippu-table env))]
    (when (or (empty? existing-nippu)
              (and (= (:kasittelytila existing-nippu)
                      (:ei-niputeta c/kasittelytilat))
                   (= (:sms_kasittelytila existing-nippu)
                      (:ei-niputeta c/kasittelytilat))
                   (:tunnus jakso)))
      (ddb/put-item
        {:ohjaaja_ytunnus_kj_tutkinto [:s oykt]
         :ohjaaja                     [:s (:ohjaaja_nimi jakso)]
         :ytunnus                     [:s (:tyopaikan_ytunnus jakso)]
         :tyopaikka                   [:s (:tyopaikan_nimi jakso)]
         :koulutuksenjarjestaja       [:s (:koulutustoimija jakso)]
         :tutkinto                    [:s (:tutkinto jakso)]
         :kasittelytila               [:s (if (:tunnus jakso)
                                            (:ei-niputettu c/kasittelytilat)
                                            (:ei-niputeta c/kasittelytilat))]
         :sms_kasittelytila           [:s (if (:tunnus jakso)
                                            (:ei-lahetetty c/kasittelytilat)
                                            (:ei-niputeta c/kasittelytilat))]
         :niputuspvm                  [:s niputuspvm]}
        {}
        (:nippu-table env))
      (swap! niput-updated inc))))

(defn -handleSupplementaryNiputus
  "Hakee jaksot tietokannasta ja käynnistää nippuluontifunktion."
  [_ _ _]
  (reset! niput-updated 0)
  (let [jaksot (ddb/query-items {:tallennuspvm [:s "2022-05-06"]}
                                {:index "supplementaryNiputusIndex"}
                                (:jaksotunnus-table env))]
    (doseq [jakso jaksot] (conditional-create-nippu jakso)))
  (log/info "Niput lisätty: " @niput-updated))
