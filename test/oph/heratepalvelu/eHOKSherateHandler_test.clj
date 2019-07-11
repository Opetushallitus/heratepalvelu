(ns oph.heratepalvelu.eHOKSherateHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.eHOKSherateHandler :refer :all]
            [oph.heratepalvelu.util :refer :all]))

(deftest test-get-koulutustoimija-oid
  (testing "Get koulutustoimija oid"
    (with-redefs
      [oph.heratepalvelu.external.organisaatio/get-organisaatio
       mock-get-organisaatio]
      (do
        (is (= "1.2.246.562.10.346830761110"
               (get-koulutustoimija-oid
                 {:oid "1.2.246.562.15.43634207518"
                  :koulutustoimija {:oid "1.2.246.562.10.346830761110"}})))
        (is (= "1.2.246.562.10.346830761110"
               (get-koulutustoimija-oid
                 {:oid "1.2.246.562.15.43634207512"
                  :oppilaitos {:oid "1.2.246.562.10.52251087186"}})))))))

(deftest test-kausi
  (testing "Generate laskentakausi string"
    (is (= "2018-2019" (kausi "2018-07-01")))
    (is (= "2018-2019" (kausi "2019-06-30")))))

(deftest test-check-suoritus-type
  (testing "Check suoritustype"
    (is (false? (check-suoritus-type {:tyyppi {:koodiarvo "valma"}})))
    (is (true? (check-suoritus-type
                 {:tyyppi {:koodiarvo "ammatillinentutkinto"}})))
    (is (true? (check-suoritus-type
                 {:tyyppi {:koodiarvo "ammatillinentutkintoosittainen"}})))))

(deftest test-check-organisaatio-whitelist
  (testing "Check organisaatio whitelist"
    (with-redefs
      [oph.heratepalvelu.db.dynamodb/get-item mock-get-item-from-whitelist]
      (do
        (is (true?
              (check-organisaatio-whitelist "1.2.246.562.10.346830761110")))
        (is (nil?
              (check-organisaatio-whitelist "1.2.246.562.10.346830761111")))
        (is (nil?
              (check-organisaatio-whitelist "1.2.246.562.10.346830761112")))))))
