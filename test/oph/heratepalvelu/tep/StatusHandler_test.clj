(ns oph.heratepalvelu.tep.StatusHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.tep.StatusHandler :as sh]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(def test-results (atom []))

(defn- add-to-test-results [data]
  (reset! test-results (cons data @test-results)))

(defn- mock-get-item [query-params table]
  (when (and (= :s (first (:ohjaaja_ytunnus_kj_tutkinto query-params)))
             (= :s (first (:niputuspvm query-params)))
             (= "2021-12-15" (second (:niputuspvm query-params)))
             (= table "nippu-table-name"))
    (add-to-test-results {:type "mock-get-item"
                          :value
                          (second (:ohjaaja_ytunnus_kj_tutkinto query-params))})
    {:ohjaaja_ytunnus_kj_tutkinto
     (second (:ohjaaja_ytunnus_kj_tutkinto query-params))
     :niputuspvm (second (:niputuspvm query-params))
     :viestintapalvelu-id
     (cond
       (= "test-id-1" (second (:ohjaaja_ytunnus_kj_tutkinto query-params))) 1
       (= "test-id-2" (second (:ohjaaja_ytunnus_kj_tutkinto query-params))) 2
       (= "test-id-3" (second (:ohjaaja_ytunnus_kj_tutkinto query-params))) 3
       (= "test-id-4" (second (:ohjaaja_ytunnus_kj_tutkinto query-params))) 4)
     :kyselylinkki
     (cond
       (= "test-id-1" (second (:ohjaaja_ytunnus_kj_tutkinto query-params)))
       "kysely.linkki/123,890"
       (= "test-id-2" (second (:ohjaaja_ytunnus_kj_tutkinto query-params)))
       "kysely.linkki/123;lkj"
       (= "test-id-3" (second (:ohjaaja_ytunnus_kj_tutkinto query-params)))
       "kysely.linkki/asdf"
       (= "test-id-4" (second (:ohjaaja_ytunnus_kj_tutkinto query-params)))
       "kysely.linkki/hjkl")
     :voimassaloppupvm "2021-11-11"}))

(defn- mock-query-items [query-params options table]
  (when (and (= :eq (first (:kasittelytila query-params)))
             (= :s (first (second (:kasittelytila query-params))))
             (= (:viestintapalvelussa c/kasittelytilat)
                (second (second (:kasittelytila query-params))))
             (= "niputusIndex" (:index options))
             (= "nippu-table-name" table))
    (add-to-test-results {:type "mock-query-items"})
    [{:ohjaaja_ytunnus_kj_tutkinto "test-id-1"
      :niputuspvm "2021-12-15"}
     {:ohjaaja_ytunnus_kj_tutkinto "test-id-2"
      :niputuspvm "2021-12-15"}
     {:ohjaaja_ytunnus_kj_tutkinto "test-id-3"
      :niputuspvm "2021-12-15"}
     {:ohjaaja_ytunnus_kj_tutkinto "test-id-4"
      :niputuspvm "2021-12-15"}]))

(defn- mock-update-nippu [nippu updates]
  (add-to-test-results {:type "mock-update-nippu"
                        :nippu nippu
                        :updates updates}))

(defn- mock-get-email-status [id]
  (add-to-test-results {:type "mock-get-email-status" :id id})
  (if (= id 3)
    {:numberOfFailedSendings 1}
    {:numberOfSuccessfulSendings 1}))

(defn- mock-patch-nippulinkki [kyselylinkki data]
  (add-to-test-results {:type "mock-patch-nippulinkki"
                        :kyselylinkki kyselylinkki
                        :data data}))

(deftest test-handleEmailStatus
  (testing "Varmista, että -handleEmailStatus kutsuu kaikkia funktioita oikein"
    (with-redefs [environ.core/env {:nippu-table "nippu-table-name"}
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 1 10))
                  oph.heratepalvelu.db.dynamodb/get-item mock-get-item
                  oph.heratepalvelu.db.dynamodb/query-items mock-query-items
                  oph.heratepalvelu.external.viestintapalvelu/get-email-status
                  mock-get-email-status
                  oph.heratepalvelu.external.arvo/patch-nippulinkki
                  mock-patch-nippulinkki
                  oph.heratepalvelu.tep.tepCommon/update-nippu
                  mock-update-nippu]
      (let [event (tu/mock-handler-event :scheduledherate)
            context (tu/mock-handler-context)
            results [{:type "mock-query-items"}
                     {:type "mock-get-item"
                      :value "test-id-1"}
                     {:type "mock-get-email-status" :id 1}
                     {:type "mock-update-nippu"
                      :nippu {:ohjaaja_ytunnus_kj_tutkinto "test-id-1"
                              :niputuspvm "2021-12-15"
                              :viestintapalvelu-id 1
                              :kyselylinkki "kysely.linkki/123,890"
                              :voimassaloppupvm "2021-11-11"}
                      :updates {:kasittelytila [:s (:success c/kasittelytilat)]
                                :voimassaloppupvm [:s "2022-02-09"]}}
                     {:type "mock-get-item"
                      :value "test-id-2"}
                     {:type "mock-get-email-status" :id 2}
                     {:type "mock-update-nippu"
                      :nippu {:ohjaaja_ytunnus_kj_tutkinto "test-id-2"
                              :niputuspvm "2021-12-15"
                              :viestintapalvelu-id 2
                              :kyselylinkki "kysely.linkki/123;lkj"
                              :voimassaloppupvm "2021-11-11"}
                      :updates {:kasittelytila [:s (:success c/kasittelytilat)]
                                :voimassaloppupvm [:s "2022-02-09"]}}
                     {:type "mock-get-item"
                      :value "test-id-3"}
                     {:type "mock-get-email-status" :id 3}
                     {:type "mock-patch-nippulinkki"
                      :kyselylinkki "kysely.linkki/asdf"
                      :data {:tila (:failed c/kasittelytilat)
                             :voimassa_loppupvm "2021-11-11"}}
                     {:type "mock-update-nippu"
                      :nippu {:ohjaaja_ytunnus_kj_tutkinto "test-id-3"
                              :niputuspvm "2021-12-15"
                              :viestintapalvelu-id 3
                              :kyselylinkki "kysely.linkki/asdf"
                              :voimassaloppupvm "2021-11-11"}
                      :updates {:kasittelytila [:s (:failed c/kasittelytilat)]
                                :voimassaloppupvm [:s "2022-02-09"]}}
                     {:type "mock-get-item"
                      :value "test-id-4"}
                     {:type "mock-get-email-status" :id 4}
                     {:type "mock-patch-nippulinkki"
                      :kyselylinkki "kysely.linkki/hjkl"
                      :data {:tila (:success c/kasittelytilat)
                             :voimassa_loppupvm "2022-02-09"}}
                     {:type "mock-update-nippu"
                      :nippu {:ohjaaja_ytunnus_kj_tutkinto "test-id-4"
                              :niputuspvm "2021-12-15"
                              :viestintapalvelu-id 4
                              :kyselylinkki "kysely.linkki/hjkl"
                              :voimassaloppupvm "2021-11-11"}
                      :updates {:kasittelytila [:s (:success c/kasittelytilat)]
                                :voimassaloppupvm [:s "2022-02-09"]}}]]
        (sh/-handleEmailStatus {} event context)
        (is (= results (vec (reverse @test-results))))))))
