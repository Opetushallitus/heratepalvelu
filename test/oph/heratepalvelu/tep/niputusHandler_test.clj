(ns oph.heratepalvelu.tep.niputusHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.tep.niputusHandler :as nh]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(deftest not-in-keskeytymisajanjakso?
  (testing "Varmistaa, että not-in-keskeytymisajanjakso? toimii oikein."
    (let [kjaksot [{:loppu (LocalDate/of 2022 4 1)}
                   {:alku  (LocalDate/of 2022 4 4)
                    :loppu (LocalDate/of 2022 4 6)}
                   {:alku  (LocalDate/of 2022 4 8)}]]
      (is (true? (nh/not-in-keskeytymisajanjakso? (LocalDate/of 2022 4 4) [])))
      (is (true? (nh/not-in-keskeytymisajanjakso? (LocalDate/of 2022 4 2)
                                                  kjaksot)))
      (is (true? (nh/not-in-keskeytymisajanjakso? (LocalDate/of 2022 4 7)
                                                  kjaksot)))
      (is (false? (nh/not-in-keskeytymisajanjakso? (LocalDate/of 2022 3 30)
                                                   kjaksot)))
      (is (false? (nh/not-in-keskeytymisajanjakso? (LocalDate/of 2022 4 4)
                                                   kjaksot)))
      (is (false? (nh/not-in-keskeytymisajanjakso? (LocalDate/of 2022 4 6)
                                                   kjaksot)))
      (is (false? (nh/not-in-keskeytymisajanjakso? (LocalDate/of 2022 4 10)
                                                   kjaksot))))))

(deftest test-filtered-jakso-days
  (testing "Varmistaa, että filtered-jakso-days toimii oikein."
    (let [test-jakso1 {:jakso_alkupvm "2022-04-27" :jakso_loppupvm "2022-05-02"}
          test-jakso2 {:jakso_alkupvm "2022-04-24" :jakso_loppupvm "2022-04-30"}
          days1 (seq [(LocalDate/of 2022 4 27)
                      (LocalDate/of 2022 4 28)
                      (LocalDate/of 2022 4 29)
                      (LocalDate/of 2022 5 2)])
          days2 (seq [(LocalDate/of 2022 4 25)
                      (LocalDate/of 2022 4 26)
                      (LocalDate/of 2022 4 27)
                      (LocalDate/of 2022 4 28)
                      (LocalDate/of 2022 4 29)])]
      (is (= (nh/filtered-jakso-days test-jakso1) days1))
      (is (= (nh/filtered-jakso-days test-jakso2) days2)))))

(deftest test-convert-keskeytymisajanjakso
  (testing "Varmistaa, että convert-keskeytymisajanjakso toimii oikein."
    (let [test1 {:alku "2022-01-01" :loppu "2022-03-03"}
          test2 {:alku "2022-06-06"}
          test3 {:loppu "2022-08-08"}
          result1 {:alku (LocalDate/of 2022 1 1) :loppu (LocalDate/of 2022 3 3)}
          result2 {:alku (LocalDate/of 2022 6 6)}
          result3 {:loppu (LocalDate/of 2022 8 8)}]
      (is (= (nh/convert-keskeytymisajanjakso test1) result1))
      (is (= (nh/convert-keskeytymisajanjakso test2) result2))
      (is (= (nh/convert-keskeytymisajanjakso test3) result3)))))

;; TODO add-to-jaksot-by-day

(deftest test-handle-one-day
  (testing "Varmistaa, että handle-one-day toimii oikein."
    (let [jaksot (seq [{:hankkimistapa-id 1 :osa-aikaisuus 100}
                       {:hankkimistapa-id 2 :osa-aikaisuus 50}
                       {:hankkimistapa-id 3 :osa-aikaisuus 0}
                       {:hankkimistapa-id 4}])
          results {1 0.25
                   2 0.125
                   3 0.0
                   4 0.25}]
      (is (= (nh/handle-one-day jaksot) results)))))

(def test-compute-kesto-results (atom []))

(deftest test-compute-kesto
  (testing "Varmistaa, että compute-kesto laskee kestot oikein."
    (with-redefs
      [oph.heratepalvelu.external.ehoks/get-tyoelamajaksot-active-between
       (fn [oppija-oid start end];; TODO
         (reset! test-compute-kesto-results
                 (cons {:type "gtab" :start start :end end}
                       @test-compute-kesto-results))
         (seq [{:hankkimistapa-id 1
                :oppija_oid "4.4.4.4"
                :osa-aikaisuus 100
                :jakso_alkupvm "2022-01-03"
                :jakso_loppupvm "2022-02-05"
                :keskeytymisajanjaksot []
                :opiskeluoikeus-oid "1.2.3.1"
                }
               {:hankkimistapa-id 2
                :oppija_oid "4.4.4.4"
                :osa-aikaisuus 50
                :jakso_alkupvm ;; TODO
                :jakso_loppupvm ;; TODO
                ;; ei keskeytymisajanjaksoja ollenkaan
                :opiskeluoikeus-oid "1.2.3.2"
                }



               ]))

       ;; TODO koski redef

       ]
      (let [jaksot [{:jakso_alkupvm "2022-01-05"
                     :Jakso_loppupvm "2022-03-03"}
                    {:jakso_alkupvm "2022-01-10"
                     :jakso_loppupvm "2022-02-28"}]
            
            results {
                     ;; TODO

                     }

            call-results [

                          ;; TODO

                          ]]
        (is (= (nh/compute-kesto jaksot) results))
        ;; TODO tarkista tallennetut hommat
      ;; TODO

      ))))

(def test-niputa-results (atom []))

(defn- add-to-test-niputa-results [data]
  (reset! test-niputa-results (cons data @test-niputa-results)))

(defn- mock-generate-uuid [] "test-uuid")

(defn- mock-niputa-query-items [query-params options table]
  (when (and (= :eq (first (:ohjaaja_ytunnus_kj_tutkinto query-params)))
             (= :s (first (second (:ohjaaja_ytunnus_kj_tutkinto query-params))))
             (= :eq (first (:niputuspvm query-params)))
             (= :s (first (second (:niputuspvm query-params))))
             (= "niputusIndex" (:index options))
             (= "#pvm >= :pvm AND attribute_exists(#tunnus)"
                (:filter-expression options))
             (= "viimeinen_vastauspvm" (get (:expr-attr-names options) "#pvm"))
             (= "tunnus" (get (:expr-attr-names options) "#tunnus"))
             (= :s (first (get (:expr-attr-vals options) ":pvm")))
             (= "jaksotunnus-table-name" table))
    (add-to-test-niputa-results
      {:type "mock-niputa-query-items"
       :pvm (second (get (:expr-attr-vals options) ":pvm"))
       :niputuspvm (second (second (:niputuspvm query-params)))
       :ohjaaja_ytunnus_kj_tutkinto
       (second (second (:ohjaaja_ytunnus_kj_tutkinto query-params)))})
    (if (not= (second (second (:ohjaaja_ytunnus_kj_tutkinto query-params)))
              "test-id-0")
      [{:tunnus "ABCDEF"
        :tyopaikan_nimi "Testi Työ Paikka"
        :viimeinen_vastauspvm "2022-02-02"}]
      [])))

(defn- mock-create-nippu-kyselylinkki [niputus-request-body]
  (add-to-test-niputa-results {:type "mock-create-nippu-kyselylinkki"
                               :niputus-request-body niputus-request-body})
  {:nippulinkki (when (= "123456-7" (:tyonantaja niputus-request-body))
                  "kysely.linkki/132")
   :voimassa_loppupvm "2021-12-17"})

(defn- mock-delete-nippukyselylinkki [tunniste]
  (add-to-test-niputa-results {:type "mock-delete-nippukyselylinkki"
                               :tunniste tunniste}))

(defn- mock-update-nippu
  ([nippu updates] (mock-update-nippu nippu updates {}))
  ([nippu updates options]
   (add-to-test-niputa-results {:type "mock-update-nippu"
                                :nippu nippu
                                :updates updates
                                :options options})))

(deftest test-niputa
  (testing "Varmista, että niputa-funktio tekee oikeita kutsuja"
    (with-redefs
      [environ.core/env {:jaksotunnus-table "jaksotunnus-table-name"
                         :nippu-table "nippu-table-name"}
       oph.heratepalvelu.common/generate-uuid mock-generate-uuid
       oph.heratepalvelu.common/local-date-now (fn [] (LocalDate/of 2021 12 31))
       oph.heratepalvelu.common/rand-str (fn [_] "abcdef")
       oph.heratepalvelu.db.dynamodb/query-items mock-niputa-query-items
       oph.heratepalvelu.external.arvo/create-nippu-kyselylinkki
       mock-create-nippu-kyselylinkki
       oph.heratepalvelu.external.arvo/delete-nippukyselylinkki
       mock-delete-nippukyselylinkki
       oph.heratepalvelu.tep.tepCommon/update-nippu mock-update-nippu]
      (let [test-nippu-0 {:ohjaaja_ytunnus_kj_tutkinto "test-id-0"
                          :niputuspvm "2021-12-15"}
            test-nippu-1 {:ohjaaja_ytunnus_kj_tutkinto "test-id-1"
                          :niputuspvm "2021-12-15"
                          :tyopaikka "Testityöpaikka"
                          :ytunnus "123456-7"
                          :koulutuksenjarjestaja "12345"
                          :tutkinto "asdf"}
            test-nippu-2 {:ohjaaja_ytunnus_kj_tutkinto "test-id-2"
                          :niputuspvm "2021-12-15"
                          :ytunnus "111111-1"
                          :koulutuksenjarjestaja "12111"
                          :tutkinto "aaaa"}
            results [{:type "mock-niputa-query-items"
                      :pvm "2021-12-31"
                      :ohjaaja_ytunnus_kj_tutkinto "test-id-0"
                      :niputuspvm "2021-12-15"}
                     {:type "mock-update-nippu"
                      :nippu {:ohjaaja_ytunnus_kj_tutkinto "test-id-0"
                              :niputuspvm "2021-12-15"}
                      :updates
                      {:kasittelytila [:s (:ei-jaksoja c/kasittelytilat)]
                       :request_id [:s "test-uuid"]
                       :kasittelypvm [:s "2021-12-31"]}
                      :options {}}
                     {:type "mock-niputa-query-items"
                      :pvm "2021-12-31"
                      :ohjaaja_ytunnus_kj_tutkinto "test-id-1"
                      :niputuspvm "2021-12-15"}
                     {:type "mock-create-nippu-kyselylinkki"
                      :niputus-request-body
                      {:tunniste "testi_tyo_paikka_2021-12-31_abcdef"
                       :koulutustoimija_oid "12345"
                       :tutkintotunnus "asdf"
                       :tyonantaja "123456-7"
                       :tyopaikka "Testityöpaikka"
                       :tunnukset (seq ["ABCDEF"])
                       :voimassa_alkupvm "2021-12-31"
                       :request_id "test-uuid"}}
                     {:type "mock-update-nippu"
                      :nippu {:ohjaaja_ytunnus_kj_tutkinto "test-id-1"
                              :niputuspvm "2021-12-15"
                              :tyopaikka "Testityöpaikka"
                              :ytunnus "123456-7"
                              :koulutuksenjarjestaja "12345"
                              :tutkinto "asdf"}
                      :updates
                      {:kasittelytila [:s (:ei-lahetetty c/kasittelytilat)]
                       :kyselylinkki [:s "kysely.linkki/132"]
                       :voimassaloppupvm [:s "2021-12-17"]
                       :request_id [:s "test-uuid"]
                       :kasittelypvm [:s "2021-12-31"]}
                      :options
                      {:cond-expr "attribute_not_exists(kyselylinkki)"}}
                     {:type "mock-niputa-query-items"
                      :pvm "2021-12-31"
                      :ohjaaja_ytunnus_kj_tutkinto "test-id-2"
                      :niputuspvm "2021-12-15"}
                     {:type "mock-create-nippu-kyselylinkki"
                      :niputus-request-body
                      {:tunniste "testi_tyo_paikka_2021-12-31_abcdef"
                       :koulutustoimija_oid "12111"
                       :tutkintotunnus "aaaa"
                       :tyonantaja "111111-1"
                       :tyopaikka nil
                       :tunnukset (seq ["ABCDEF"])
                       :voimassa_alkupvm "2021-12-31"
                       :request_id "test-uuid"}}
                     {:type "mock-update-nippu"
                      :nippu {:ohjaaja_ytunnus_kj_tutkinto "test-id-2"
                              :niputuspvm "2021-12-15"
                              :ytunnus "111111-1"
                              :koulutuksenjarjestaja "12111"
                              :tutkinto "aaaa"}
                      :updates
                      {:kasittelytila [:s (:niputusvirhe c/kasittelytilat)]
                       :kasittelypvm [:s "2021-12-31"]
                       :reason [:s "no reason in response"]
                       :request_id [:s "test-uuid"]}
                      :options {}}]]
        (nh/niputa test-nippu-0)
        (nh/niputa test-nippu-1)
        (nh/niputa test-nippu-2)
        (is (= results (vec (reverse @test-niputa-results))))))))

(def handler-results (atom []))

(defn- add-to-handler-results [data]
  (reset! handler-results (cons data @handler-results)))

(defn- mock-handler-query-items [query-params options table]
  (when (and (= :eq (first (:kasittelytila query-params)))
             (= :s (first (second (:kasittelytila query-params))))
             (= (:ei-niputettu c/kasittelytilat)
                (second (second (:kasittelytila query-params))))
             (= :le (first (:niputuspvm query-params)))
             (= :s (first (second (:niputuspvm query-params))))
             (= "niputusIndex" (:index options))
             (= 10 (:limit options))
             (= "nippu-table-name" table))
    (add-to-handler-results
      {:type "mock-handler-query-items"
       :date (second (second (:niputuspvm query-params)))})
    [{:id 2 :niputuspvm "2021-11-30"}
     {:id 1 :niputuspvm "2021-11-11"}
     {:id 3 :niputuspvm "2021-12-10"}]))

(defn- mock-niputa [nippu]
  (add-to-handler-results {:type "mock-niputa" :nippu nippu}))

(deftest test-handleNiputus
  (testing "Varmista, että -handleNiputus tekee oikeita kutsuja"
    (with-redefs
      [environ.core/env {:nippu-table "nippu-table-name"}
       oph.heratepalvelu.common/local-date-now (fn [] (LocalDate/of 2021 12 15))
       oph.heratepalvelu.db.dynamodb/query-items mock-handler-query-items
       oph.heratepalvelu.tep.niputusHandler/niputa mock-niputa]
      (let [event (tu/mock-handler-event :scheduledherate)
            context (tu/mock-handler-context)
            results [{:type "mock-handler-query-items"
                      :date "2021-12-15"}
                     {:type "mock-niputa"
                      :nippu {:id 3
                              :niputuspvm "2021-12-10"}}
                     {:type "mock-niputa"
                      :nippu {:id 2
                              :niputuspvm "2021-11-30"}}
                     {:type "mock-niputa"
                      :nippu {:id 1
                              :niputuspvm "2021-11-11"}}]]
        (nh/-handleNiputus {} event context)
        (is (= results (vec (reverse @handler-results))))))))
