(ns oph.heratepalvelu.tep.tepCommon-test
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

(defn- mock-get-jaksot-for-nippu-query-items [query-params options table]
  (when (and (= :eq (first (:ohjaaja_ytunnus_kj_tutkinto query-params)))
             (= :s (first (second (:ohjaaja_ytunnus_kj_tutkinto query-params))))
             (= :eq (first (:niputuspvm query-params)))
             (= :s (first (second (:niputuspvm query-params))))
             (= "niputusIndex" (:index options))
             (= "jaksotunnus-table-name" table))
    {:ohjaaja_ytunnus_kj_tutkinto (second (second (:ohjaaja_ytunnus_kj_tutkinto
                                                    query-params)))
     :niputuspvm (second (second (:niputuspvm query-params)))}))

(deftest test-get-jaksot-for-nippu
  (testing "Varmista, että do-jakso-query kutsuu query-items oikein"
    (with-redefs [environ.core/env {:jaksotunnus-table "jaksotunnus-table-name"}
                  oph.heratepalvelu.db.dynamodb/query-items
                  mock-get-jaksot-for-nippu-query-items]
      (let [nippu {:ohjaaja_ytunnus_kj_tutkinto "test-nippu-id"
                   :niputuspvm "2021-10-10"}]
        (is (= (tc/get-jaksot-for-nippu nippu) nippu))))))

(deftest test-reduce-common-value
  (testing (str "Varmista, että reduce-common-value palauttaa yhteisen arvon, "
                "jos kaikissa itemeissä on sama arvo tai nil; muuten nil.")
    (let [same-emails [{:ohjaaja_email "a@b.com"}
                       {:ohjaaja_email "a@b.com"}]
          different-emails [{:ohjaaja_email "a@b.com"}
                            {:ohjaaja_email "x@y.com"}]
          with-nil [{:field "asdf"}
                    {}
                    {:field "asdf"}]
          one-item [{:field "asdf"}]
          no-items []]
      (is (= "a@b.com" (tc/reduce-common-value same-emails :ohjaaja_email)))
      (is (nil? (tc/reduce-common-value different-emails :ohjaaja_email)))
      (is (= "asdf" (tc/reduce-common-value with-nil :field)))
      (is (= "asdf" (tc/reduce-common-value one-item :field)))
      (is (nil? (tc/reduce-common-value no-items :field))))))
