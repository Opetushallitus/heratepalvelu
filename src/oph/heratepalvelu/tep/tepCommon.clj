(ns oph.heratepalvelu.tep.tepCommon
  "Yhteiset funktiot TEP-puolelle."
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb])
  (:import (java.time LocalDate)
           (software.amazon.awssdk.awscore.exception AwsServiceException)))

(defn get-new-loppupvm
  "Laskee uuden loppupäivämäärän nipulle, jos kyselyä ei ole lähetetty ja siihen
  ei ole vastattu. Palauttaa muuten nil."
  ([nippu] (get-new-loppupvm nippu (c/local-date-now)))
  ([nippu ^LocalDate date]
   (when-not (or (= (:kasittelytila nippu) (:success c/kasittelytilat))
                 (= (:kasittelytila nippu) (:vastattu c/kasittelytilat))
                 (= (:sms_kasittelytila nippu) (:success c/kasittelytilat))
                 (= (:sms_kasittelytila nippu) (:vastattu c/kasittelytilat)))
     (let [new-loppupvm (.plusDays date 30)
           takaraja (.plusDays (LocalDate/parse (:niputuspvm nippu)) 60)]
       (str (if (c/is-before takaraja new-loppupvm) takaraja new-loppupvm))))))

(defn get-jaksot-for-nippu
  "Hakee nippuun liittyvät jaksot tietokannasta."
  [nippu]
  (try
    (ddb/query-items
      {:ohjaaja_ytunnus_kj_tutkinto [:eq
                                     [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]]
       :niputuspvm                  [:eq [:s (:niputuspvm nippu)]]}
      {:index "niputusIndex"
       :filter-expression "attribute_not_exists(mitatoity)"}
      (:jaksotunnus-table env))
    (catch AwsServiceException e
      (log/error "Jakso-query epäonnistui nipulla" nippu)
      (log/error e))))

(defn reduce-common-value
  "Jos kaikissa annetuissa objekteissa (items) on kentässä f sama arvo tai nil,
  palauttaa yhteisen arvon. Muuten palauttaa nil."
  [items f]
  (let [values (set (keep f items))]
    (if (= 1 (count values)) (first values))))

(defn update-nippu
  "Wrapper update-itemin ympäri, joka yksinkertaistaa tietokantapäivitykset."
  ([nippu updates] (update-nippu nippu updates {}))
  ([nippu updates options]
   (ddb/update-item
     {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]
      :niputuspvm                  [:s (:niputuspvm nippu)]}
     (merge (c/create-update-item-options updates) options)
     (:nippu-table env))))

(defn update-jakso
  "Wrapper update-itemin ympäri, joka yksinkertaistaa tietokantapäivitykset."
  ([jakso updates] (update-jakso jakso updates {}))
  ([jakso updates options]
   (ddb/update-item {:hankkimistapa_id [:n (:hankkimistapa_id jakso)]}
                    (merge (c/create-update-item-options updates) options)
                    (:jaksotunnus-table env))))
