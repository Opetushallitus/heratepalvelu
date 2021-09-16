(ns oph.heratepalvelu.tep.tepCommon
  (:require [oph.heratepalvelu.common :as c])
  (:import (java.time LocalDate)))

(defn get-new-loppupvm
  ([nippu] (get-new-loppupvm nippu (LocalDate/now)))
  ([nippu date]
    (if (or (= (:kasittelytila nippu) (:success c/kasittelytilat))
            (= (:kasittelytila nippu) (:vastattu c/kasittelytilat))
            (= (:sms_kasittelytila nippu) (:success c/kasittelytilat))
            (= (:sms_kasittelytila nippu) (:vastattu c/kasittelytilat)))
      nil
      (let [new-loppupvm (.plusDays date 30)
            takaraja (.plusDays (c/to-date (:niputuspvm nippu)) 60)]
        (str (if (.isBefore takaraja new-loppupvm) takaraja new-loppupvm))))))
