(ns oph.heratepalvelu.integration-tests.tpk.tpkNiputusHandler-i-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.integration-tests.mock-db :as mdb]
            [oph.heratepalvelu.tpk.tpkNiputusHandler :as tnh]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(def mock-env {:jaksotunnus-table "jaksotunnus-table-name"
               :tpk-nippu-table "tpk-nippu-table-name"})

(def starting-jaksotunnus-table [{:hankkimistapa_id [:n 123]
                                  :tyopaikan_nimi [:s "Työ Paikka"]
                                  :tyopaikan_ytunnus [:s "123456-7"]
                                  :koulutustoimija [:s "test-kt"]
                                  :hankkimistapa_tyyppi [:s "koulutussopimus"]
                                  :jakso_loppupvm [:s "2021-09-09"]
                                  :tpk-niputuspvm [:s "ei_maaritelty"]}
                                 {:hankkimistapa_id [:n 124]
                                  :tyopaikan_nimi [:s "Työ Paikka"]
                                  :tyopaikan_ytunnus [:s "123456-7"]
                                  :koulutustoimija [:s "test-kt"]
                                  :hankkimistapa_tyyppi [:s "oppisopimus"]
                                  :oppisopimuksen_perusta [:s "01"]
                                  :jakso_loppupvm [:s "2021-09-09"]
                                  :tpk-niputuspvm [:s "ei_maaritelty"]}
                                 {:hankkimistapa_id [:n 222]
                                  :tyopaikan_nimi [:s "Ääkköset"]
                                  :tyopaikan_ytunnus [:s "333333-7"]
                                  :koulutustoimija [:s "test-kt2"]
                                  :hankkimistapa_tyyppi [:s "koulutussopimus"]
                                  :jakso_loppupvm [:s "2021-08-01"]
                                  :tpk-niputuspvm [:s "ei_maaritelty"]}])

(def starting-tpk-nippu-table
  [{:nippu-id [:s "aakkoset/333333-7/test-kt2/2021-07-01_2021-12-31"]
    :tyopaikan-nimi [:s "Ääkköset"]
    :tyopaikan-nimi-normalisoitu [:s "aakkoset"]
    :tyopaikan-ytunnus [:s "333333-7"]
    :koulutustoimija-oid [:s "test-kt2"]
    :tiedonkeruu-alkupvm [:s "2021-07-01"]
    :tiedonkeruu-loppupvm [:s "2021-12-31"]
    :vastaamisajan-alkupvm [:s "2022-01-01"]
    :vastaamisajan-loppupvm [:s "2022-02-28"]
    :niputuspvm [:s "2021-07-25"]}])

(defn- setup-test []
  (mdb/clear-mock-db)
  (mdb/create-table (:jaksotunnus-table mock-env)
                    {:primary-key :hankkimistapa_id})
  (mdb/set-table-contents (:jaksotunnus-table mock-env)
                          starting-jaksotunnus-table)
  (mdb/create-table (:tpk-nippu-table mock-env)
                    {:primary-key :nippu-id
                     :sort-key :tiedonkeruu-alkupvm})
  (mdb/set-table-contents (:tpk-nippu-table mock-env)
                          starting-tpk-nippu-table))

(def expected-jaksotunnus-table #{{:hankkimistapa_id [:n 123]
                                   :tyopaikan_nimi [:s "Työ Paikka"]
                                   :tyopaikan_ytunnus [:s "123456-7"]
                                   :koulutustoimija [:s "test-kt"]
                                   :hankkimistapa_tyyppi [:s "koulutussopimus"]
                                   :jakso_loppupvm [:s "2021-09-09"]
                                   :tpk-niputuspvm [:s "2022-01-01"]}
                                  {:hankkimistapa_id [:n 124]
                                   :tyopaikan_nimi [:s "Työ Paikka"]
                                   :tyopaikan_ytunnus [:s "123456-7"]
                                   :koulutustoimija [:s "test-kt"]
                                   :hankkimistapa_tyyppi [:s "oppisopimus"]
                                   :oppisopimuksen_perusta [:s "01"]
                                   :jakso_loppupvm [:s "2021-09-09"]
                                   :tpk-niputuspvm [:s "2022-01-01"]}
                                  {:hankkimistapa_id [:n 222]
                                   :tyopaikan_nimi [:s "Ääkköset"]
                                   :tyopaikan_ytunnus [:s "333333-7"]
                                   :koulutustoimija [:s "test-kt2"]
                                   :hankkimistapa_tyyppi [:s "koulutussopimus"]
                                   :jakso_loppupvm [:s "2021-08-01"]
                                   :tpk-niputuspvm [:s "2021-07-25"]}})

(def expected-tpk-nippu-table
  #{{:nippu-id [:s "tyo_paikka/123456-7/test-kt/2021-07-01_2021-12-31"]
     :tyopaikan-nimi [:s "Työ Paikka"]
     :tyopaikan-nimi-normalisoitu [:s "tyo_paikka"]
     :tyopaikan-ytunnus [:s "123456-7"]
     :koulutustoimija-oid [:s "test-kt"]
     :tiedonkeruu-alkupvm [:s "2021-07-01"]
     :tiedonkeruu-loppupvm [:s "2021-12-31"]
     :vastaamisajan-alkupvm [:s "2022-01-01"]
     :vastaamisajan-loppupvm [:s "2022-02-28"]
     :niputuspvm [:s "2022-01-01"]}
    {:nippu-id [:s "aakkoset/333333-7/test-kt2/2021-07-01_2021-12-31"]
     :tyopaikan-nimi [:s "Ääkköset"]
     :tyopaikan-nimi-normalisoitu [:s "aakkoset"]
     :tyopaikan-ytunnus [:s "333333-7"]
     :koulutustoimija-oid [:s "test-kt2"]
     :tiedonkeruu-alkupvm [:s "2021-07-01"]
     :tiedonkeruu-loppupvm [:s "2021-12-31"]
     :vastaamisajan-alkupvm [:s "2022-01-01"]
     :vastaamisajan-loppupvm [:s "2022-02-28"]
     :niputuspvm [:s "2021-07-25"]}})

(deftest test-tpkNiputusHandler-integration
  (testing "tpkNiputusHandler integraatiotesti"
    (with-redefs [environ.core/env mock-env
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 1 1))
                  oph.heratepalvelu.db.dynamodb/get-item mdb/get-item
                  oph.heratepalvelu.db.dynamodb/put-item mdb/put-item
                  oph.heratepalvelu.db.dynamodb/scan mdb/scan
                  oph.heratepalvelu.db.dynamodb/update-item mdb/update-item]
      (setup-test)
      (tnh/-handleTpkNiputus {}
                             (tu/mock-handler-event :scheduledherate)
                             (tu/mock-handler-context 40000))
      (is (= (mdb/get-table-values (:jaksotunnus-table mock-env))
             expected-jaksotunnus-table))
      (is (= (mdb/get-table-values (:tpk-nippu-table mock-env))
             expected-tpk-nippu-table))
      (mdb/clear-mock-db))))
