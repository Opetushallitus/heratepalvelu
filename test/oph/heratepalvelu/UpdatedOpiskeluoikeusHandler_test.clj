(ns oph.heratepalvelu.UpdatedOpiskeluoikeusHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.UpdatedOpiskeluoikeusHandler :refer :all]))

(deftest test-get-vahvistus-pvm
  (testing "Get vahvistus pvm"
    (is (= (get-vahvistus-pvm {:oid "1.2.246.562.15.82039738925"
                               :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
                               :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                                              :tyyppi {:koodiarvo "ammatillinentutkinto"}
                                              :vahvistus {:päivä "2019-07-24"}}]})
           "2019-07-24"))
    (is (nil? (get-vahvistus-pvm {:oid "1.2.246.562.15.82039738925"
                               :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
                               :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                                              :tyyppi {:koodiarvo "valma"}
                                              :vahvistus {:päivä "2019-07-24"}}]})))
    (is (nil? (get-vahvistus-pvm {:oid "1.2.246.562.15.82039738925"
                                  :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
                                  :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                                                 :tyyppi {:koodiarvo "valma"}}]})))
    (is (= (get-vahvistus-pvm {:oid "1.2.246.562.15.82039738925"
                               :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
                               :suoritukset [{:suorituskieli {:koodiarvo "FI"}
                                              :tyyppi {:koodiarvo "nayttotutkintoonvalmistavakoulutus"}
                                              :vahvistus {:päivä "2019-07-24"}}
                                             {:suorituskieli {:koodiarvo "FI"}
                                              :tyyppi {:koodiarvo "ammatillinentutkinto"}
                                              :vahvistus {:päivä "2019-07-23"}}]})
           "2019-07-23"))))

(deftest test-get-kysely-type
  (testing "Get correct kyselytyyppi from suoritus"
    (is (= (get-kysely-type {:tyyppi {:koodiarvo "ammatillinentutkinto"}})
            "tutkinnon_suorittaneet"))
    (is (= (get-kysely-type {:tyyppi {:koodiarvo "ammatillinentutkintoosittainen"}})
           "tutkinnon_osia_suorittaneet"))
    (is (nil? (get-kysely-type {:tyyppi {:koodiarvo "adaaf"}})))))