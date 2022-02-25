(ns oph.heratepalvelu.external.viestintapalvelu-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]))

(deftest test-convert-email-status
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
      (is (= (:success c/kasittelytilat) (vp/convert-email-status status1)))
      (is (= (:bounced c/kasittelytilat) (vp/convert-email-status status2)))
      (is (= (:failed c/kasittelytilat) (vp/convert-email-status status3)))
      (is (= nil (vp/convert-email-status status4))))))
