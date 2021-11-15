(ns oph.heratepalvelu.amis.EmailStatusHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.amis.EmailStatusHandler :as esh]))

(deftest test-convert-vp-email-status
  (testing "Converts viestintäpalvelu email status to internal tila"
    (let [status1 {:numberOfSuccessfulSendings 1
                   :numberOfBouncedSendings 0
                   :numberOfFailedSendings 0}
          status2 {:numberOfSuccessfulSendings 0
                   :numberOfBouncedSendings 1
                   :numberOfFailedSendings 0}
          status3 {:numberOfSuccessfulSendings 0
                   :numberOfBouncedSendings 0
                   :numberOfFailedSendings 1}
          status4 {:numberOfSuccessfulSendings 0
                   :numberOfBouncedSendings 0
                   :numberOfFailedSendings 0}]
      (is (= (:success c/kasittelytilat) (esh/convert-vp-email-status status1)))
      (is (= (:bounced c/kasittelytilat) (esh/convert-vp-email-status status2)))
      (is (= (:failed c/kasittelytilat) (esh/convert-vp-email-status status3)))
      (is (= nil (esh/convert-vp-email-status status4))))))
