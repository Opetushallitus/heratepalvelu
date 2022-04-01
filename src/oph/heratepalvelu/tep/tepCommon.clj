(ns oph.heratepalvelu.tep.tepCommon
  "Yhteiset funktiot TEP-puolelle."
  (:require [clojure.string :as s]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.organisaatio :as org])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))

(defn get-new-loppupvm
  "Laskee uuden loppupäivämäärän nipulle, jos kyselyä ei ole lähetetty ja siihen
  ei ole vastattu. Palauttaa muuten nil."
  ([nippu] (get-new-loppupvm nippu (c/local-date-now)))
  ([nippu date]
   (when-not (or (= (:kasittelytila nippu) (:success c/kasittelytilat))
                 (= (:kasittelytila nippu) (:vastattu c/kasittelytilat))
                 (= (:sms_kasittelytila nippu) (:success c/kasittelytilat))
                 (= (:sms_kasittelytila nippu) (:vastattu c/kasittelytilat)))
     (let [new-loppupvm (.plusDays date 30)
           takaraja (.plusDays (c/to-date (:niputuspvm nippu)) 60)]
       (str (if (.isBefore takaraja new-loppupvm) takaraja new-loppupvm))))))

(defn get-jaksot-for-nippu
  "Hakee nippuun liittyvät jaksot tietokannasta."
  [nippu]
  (try
    (ddb/query-items
      {:ohjaaja_ytunnus_kj_tutkinto [:eq
                                     [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]]
       :niputuspvm                  [:eq [:s (:niputuspvm nippu)]]}
      {:index "niputusIndex"}
      (:jaksotunnus-table env))
    (catch AwsServiceException e
      (log/error "Jakso-query epäonnistui nipulla" nippu)
      (log/error e))))

(defn reduce-common-value
  "Jos kaikissa annetuissa objekteissa (items) on kentässä f sama arvo tai nil,
  palauttaa yhteisen arvon. Muuten palauttaa nil."
  [items f]
  (reduce #(if (some? %1)
             (if (and (some? (f %2)) (not= %1 (f %2))) (reduced nil) %1)
             (f %2))
          nil
          items))

(defn get-oppilaitokset
  "Hakee oppilaitosten nimet organisaatiopalvelusta jaksojen oppilaiton-kentän
  perusteella."
  [jaksot]
  (try
    (seq (set (map #(:nimi (org/get-organisaatio (:oppilaitos %1))) jaksot)))
    (catch Exception e
      (log/error "Virhe kutsussa organisaatiopalveluun")
      (log/error e))))

(defn update-nippu
  "Wrapper update-itemin ympäri, joka yksinkertaistaa tietokantapäivitykset."
  ([nippu updates] (update-nippu nippu updates {}))
  ([nippu updates options]
   (ddb/update-item
     {:ohjaaja_ytunnus_kj_tutkinto [:s (:ohjaaja_ytunnus_kj_tutkinto nippu)]
      :niputuspvm                  [:s (:niputuspvm nippu)]}
     (merge (c/create-update-item-options updates) options)
     (:nippu-table env))))
