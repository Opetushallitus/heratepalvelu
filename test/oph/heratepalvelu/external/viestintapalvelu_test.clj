(ns oph.heratepalvelu.external.viestintapalvelu-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]))

(deftest test-viestintapalvelu-status->kasittelytila
  (testing "viestintäpalvelu email status response to internal tila"
    (are [kasittelytila status]
         (= kasittelytila (vp/viestintapalvelu-status->kasittelytila status))
      (:success c/kasittelytilat) {:numberOfSuccessfulSendings 1
                                   :numberOfBouncedSendings 0
                                   :numberOfFailedSendings 0}
      (:bounced c/kasittelytilat) {:numberOfSuccessfulSendings 0
                                   :numberOfBouncedSendings 1
                                   :numberOfFailedSendings 0}
      (:failed c/kasittelytilat) {:numberOfSuccessfulSendings 0
                                  :numberOfBouncedSendings 0
                                  :numberOfFailedSendings 1}
      nil {:numberOfSuccessfulSendings 0
           :numberOfBouncedSendings 0
           :numberOfFailedSendings 0})))
