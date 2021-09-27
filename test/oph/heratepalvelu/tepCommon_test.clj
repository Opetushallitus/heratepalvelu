(ns oph.heratepalvelu.tepCommon-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.tep.tepCommon :as tc]))

(deftest get-new-loppupvm-test
  (testing "Varmistaa, että get-new-loppupvm palauttaa oikean päivämäärän"
    (let [date-str "2021-09-15"
          date (c/to-date date-str)
          email-sent {:kasittelytila (:success c/kasittelytilat)}
          email-vastattu {:kasittelytila (:vastattu c/kasittelytilat)}
          sms-sent {:sms_kasittelytila (:success c/kasittelytilat)}
          sms-vastattu {:sms_kasittelytila (:vastattu c/kasittelytilat)}
          not-sent {:niputuspvm date-str}]
      (is (= (tc/get-new-loppupvm email-sent) nil))
      (is (= (tc/get-new-loppupvm email-vastattu) nil))
      (is (= (tc/get-new-loppupvm sms-sent) nil))
      (is (= (tc/get-new-loppupvm sms-vastattu) nil))
      (is (= (tc/get-new-loppupvm not-sent (.plusDays date 10))
             (str (.plusDays date 40))))
      (is (= (tc/get-new-loppupvm not-sent (.plusDays date 45))
             (str (.plusDays date 60)))))))
