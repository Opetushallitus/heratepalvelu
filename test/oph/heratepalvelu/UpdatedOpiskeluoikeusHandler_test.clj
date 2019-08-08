(ns oph.heratepalvelu.UpdatedOpiskeluoikeusHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.UpdatedOpiskeluoikeusHandler :refer :all]))

(deftest test-get-vahvistus-pvm
  (testing "Get vahvistus pvm"
    (is (get-vahvistus-pvm {:oid "1.2.246.562.15.82039738925"
                            :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
                            :suoritukset [
                                          {:suorituskieli {:koodiarvo "FI"}
                                           :tyyppi {:koodiarvo "ammatillinentutkinto"}
                                           :vahvistus {:päivä "2019-07-24"}}]})
        "2019-07-24")))

(deftest test-date-string-to-timestamp
  (testing "Transforming date-string to timestamp")
  (is (date-string-to-timestamp "1970-01-01") 0)
  (is (date-string-to-timestamp "2018-08-01") 1564617600))
