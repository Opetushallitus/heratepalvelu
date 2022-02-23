(ns oph.heratepalvelu.tep.tepCommon
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.db.dynamodb :as ddb])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))

(defn get-new-loppupvm
  "Laskee uuden loppupäivämäärän nipulle, jos kyselyä ei ole lähetetty ja siihen
  ei ole vastattu. Palauttaa muuten nil."
  ([nippu] (get-new-loppupvm nippu (c/local-date-now)))
  ([nippu date]
    (if (or (= (:kasittelytila nippu) (:success c/kasittelytilat))
            (= (:kasittelytila nippu) (:vastattu c/kasittelytilat))
            (= (:sms_kasittelytila nippu) (:success c/kasittelytilat))
            (= (:sms_kasittelytila nippu) (:vastattu c/kasittelytilat)))
      nil
      (let [new-loppupvm (.plusDays date 30)
            takaraja (.plusDays (c/to-date (:niputuspvm nippu)) 60)]
        (str (if (.isBefore takaraja new-loppupvm) takaraja new-loppupvm))))))

(defn get-jaksot-for-nippu [nippu]
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
