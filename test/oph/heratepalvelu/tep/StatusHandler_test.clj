(ns oph.heratepalvelu.tep.StatusHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.tep.StatusHandler :as sh]
            [oph.heratepalvelu.test-util :as tu]))

(deftest test-convert-email-status
  (testing "Varmista, että convert-email-status palauttaa oikeat arvot"
    (let [success-status {:numberOfSuccessfulSendings 1}
          bounced-status {:numberOfBouncedSendings 1}
          failed-status {:numberOfFailedSendings 1}
          other-status {:somethingElse 5}]
      (is (= (sh/convert-email-status success-status)
             (:success c/kasittelytilat)))
      (is (= (sh/convert-email-status bounced-status)
             (:bounced c/kasittelytilat)))
      (is (= (sh/convert-email-status failed-status)
             (:failed c/kasittelytilat)))
      (is (nil? (sh/convert-email-status other-status))))))

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
    (add-to-test-results {:type "mock-query-items"
                          :limit (:limit options)})
    [{:ohjaaja_ytunnus_kj_tutkinto "test-id-1"
      :niputuspvm "2021-12-15"}
     {:ohjaaja_ytunnus_kj_tutkinto "test-id-2"
      :niputuspvm "2021-12-15"}
     {:ohjaaja_ytunnus_kj_tutkinto "test-id-3"
      :niputuspvm "2021-12-15"}
     {:ohjaaja_ytunnus_kj_tutkinto "test-id-4"
      :niputuspvm "2021-12-15"}]))

(defn- mock-update-item [query-params options table]
  (when (and (= :s (first (:ohjaaja_ytunnus_kj_tutkinto query-params)))
             (= :s (first (:niputuspvm query-params)))
             (= "2021-12-15" (second (:niputuspvm query-params)))
             (= "SET #kasittelytila = :kasittelytila, #loppupvm = :loppupvm"
                (:update-expr options))
             (= "kasittelytila"
                (get (:expr-attr-names options) "#kasittelytila"))
             (= "voimassaloppupvm" (get (:expr-attr-names options) "#loppupvm"))
             (= :s (first (get (:expr-attr-vals options) ":kasittelytila")))
             (= :s (first (get (:expr-attr-vals options) ":loppupvm")))
             (= "nippu-table-name" table))
    (add-to-test-results
      {:type "mock-update-item"
       :values {:ohjaaja_ytunnus_kj_tutkinto
                (second (:ohjaaja_ytunnus_kj_tutkinto query-params))
                :kasittelytila (second (get (:expr-attr-vals options)
                                            ":kasittelytila"))
                :loppupvm (second (get (:expr-attr-vals options)
                                       ":loppupvm"))}})))

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
                  oph.heratepalvelu.db.dynamodb/get-item mock-get-item
                  oph.heratepalvelu.db.dynamodb/query-items mock-query-items
                  oph.heratepalvelu.db.dynamodb/update-item mock-update-item
                  oph.heratepalvelu.external.viestintapalvelu/get-email-status
                  mock-get-email-status
                  oph.heratepalvelu.external.arvo/patch-nippulinkki
                  mock-patch-nippulinkki]
      (let [event (tu/mock-handler-event :scheduledherate)
            context (tu/mock-handler-context)
            results [{:type "mock-query-items"
                      :limit 100}
                     {:type "mock-get-item"
                      :value "test-id-1"}
                     {:type "mock-get-email-status" :id 1}
                     {:type "mock-update-item"
                      :values {:ohjaaja_ytunnus_kj_tutkinto "test-id-1"
                               :kasittelytila (:success c/kasittelytilat)
                               :loppupvm "2022-02-09"}}
                     {:type "mock-get-item"
                      :value "test-id-2"}
                     {:type "mock-get-email-status" :id 2}
                     {:type "mock-update-item"
                      :values {:ohjaaja_ytunnus_kj_tutkinto "test-id-2"
                               :kasittelytila (:success c/kasittelytilat)
                               :loppupvm "2022-02-09"}}
                     {:type "mock-get-item"
                      :value "test-id-3"}
                     {:type "mock-get-email-status" :id 3}
                     {:type "mock-patch-nippulinkki"
                      :kyselylinkki "kysely.linkki/asdf"
                      :data {:tila (:failed c/kasittelytilat)}}
                     {:type "mock-update-item"
                      :values {:ohjaaja_ytunnus_kj_tutkinto "test-id-3"
                               :kasittelytila (:failed c/kasittelytilat)
                               :loppupvm "2022-02-09"}}
                     {:type "mock-get-item"
                      :value "test-id-4"}
                     {:type "mock-get-email-status" :id 4}
                     {:type "mock-patch-nippulinkki"
                      :kyselylinkki "kysely.linkki/hjkl"
                      :data {:tila (:success c/kasittelytilat)
                             :voimassa_loppupvm "2022-02-09"}}
                     {:type "mock-update-item"
                      :values {:ohjaaja_ytunnus_kj_tutkinto "test-id-4"
                               :kasittelytila (:success c/kasittelytilat)
                               :loppupvm "2022-02-09"}}]]
        (sh/-handleEmailStatus {} event context)
        (is (= results (vec (reverse @test-results))))))))
