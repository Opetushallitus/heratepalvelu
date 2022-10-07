(ns oph.heratepalvelu.tep.niputusHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.tep.niputusHandler :as nh]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(deftest test-not-in-keskeytymisajanjakso?
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

(deftest test-is-weekday?
  (testing "Varmistaa, että is-weekday? palauttaa true jos kyse on arkipäivästä"
    (is (true? (nh/is-weekday? (LocalDate/of 2022 6 13))))
    (is (true? (nh/is-weekday? (LocalDate/of 2022 6 10))))
    (is (false? (nh/is-weekday? (LocalDate/of 2022 6 4))))
    (is (false? (nh/is-weekday? (LocalDate/of 2022 6 5))))))

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

(deftest test-add-to-jaksot-by-day
  (testing "Varmistaa, että add-to-jaksot-by-day toimii oikein."
    (let [opiskeluoikeus {:tila
                          {:opiskeluoikeusjaksot
                           [{:alku "2022-01-01" :tila {:koodiarvo "lasna"}}
                            {:alku "2022-01-17" :tila {:koodiarvo "loma"}}
                            {:alku "2022-01-19" :tila {:koodiarvo "lasna"}}
                            {:alku "2022-01-21"
                             :tila {:koodiarvo "valiaikaisestikeskeytynyt"}}
                            {:alku "2022-02-25" :tila {:koodiarvo "lasna"}}]}}
          existing-jakso {:jakso_alkupvm "2022-01-10"
                          :jakso_loppupvm "2022-01-16"}
          jaksot-by-day {(LocalDate/of 2022 1 10) (seq [existing-jakso])
                         (LocalDate/of 2022 1 11) (seq [existing-jakso])
                         (LocalDate/of 2022 1 12) (seq [existing-jakso])
                         (LocalDate/of 2022 1 13) (seq [existing-jakso])
                         (LocalDate/of 2022 1 14) (seq [existing-jakso])}
          jakso {:jakso_alkupvm "2022-01-09"
                 :jakso_loppupvm "2022-01-24"
                 :keskeytymisajanjaksot [{:alku "2022-01-12"
                                          :loppu "2022-01-12"}]
                 :opiskeluoikeus_oid "asdf"}
          results {(LocalDate/of 2022 1 10) (seq [jakso existing-jakso])
                   (LocalDate/of 2022 1 11) (seq [jakso existing-jakso])
                   (LocalDate/of 2022 1 12) (seq [existing-jakso])
                   (LocalDate/of 2022 1 13) (seq [jakso existing-jakso])
                   (LocalDate/of 2022 1 14) (seq [jakso existing-jakso])
                   (LocalDate/of 2022 1 19) (seq [jakso])
                   (LocalDate/of 2022 1 20) (seq [jakso])}]
      (is (= (nh/add-to-jaksot-by-day jaksot-by-day jakso opiskeluoikeus)
             results)))))

(deftest test-add-to-jaksot-by-day-new
  (testing "Varmistaa, että add-to-jaksot-by-day-new toimii oikein."
    (let [opiskeluoikeus {:tila
                          {:opiskeluoikeusjaksot
                           [{:alku "2022-01-01" :tila {:koodiarvo "lasna"}}
                            {:alku "2022-01-17" :tila {:koodiarvo "loma"}}
                            {:alku "2022-01-19" :tila {:koodiarvo "lasna"}}
                            {:alku "2022-01-21"
                             :tila {:koodiarvo "valiaikaisestikeskeytynyt"}}
                            {:alku "2022-02-25" :tila {:koodiarvo "lasna"}}]}}
          existing-jakso {:jakso_alkupvm "2022-01-10"
                          :jakso_loppupvm "2022-01-16"}
          jaksot-by-day {(LocalDate/of 2022 1 10) (seq [existing-jakso])
                         (LocalDate/of 2022 1 11) (seq [existing-jakso])
                         (LocalDate/of 2022 1 12) (seq [existing-jakso])
                         (LocalDate/of 2022 1 13) (seq [existing-jakso])
                         (LocalDate/of 2022 1 14) (seq [existing-jakso])
                         (LocalDate/of 2022 1 15) (seq [existing-jakso])
                         (LocalDate/of 2022 1 16) (seq [existing-jakso])}
          jakso {:jakso_alkupvm "2022-01-09"
                 :jakso_loppupvm "2022-01-28"
                 :keskeytymisajanjaksot [{:alku "2022-01-12"
                                          :loppu "2022-01-12"}]
                 :opiskeluoikeus_oid "asdf"}
          results {(LocalDate/of 2022 1  9) (seq [jakso])
                   (LocalDate/of 2022 1 10) (seq [jakso existing-jakso])
                   (LocalDate/of 2022 1 11) (seq [jakso existing-jakso])
                   (LocalDate/of 2022 1 12) (seq [existing-jakso])
                   (LocalDate/of 2022 1 13) (seq [jakso existing-jakso])
                   (LocalDate/of 2022 1 14) (seq [jakso existing-jakso])
                   (LocalDate/of 2022 1 15) (seq [jakso existing-jakso])
                   (LocalDate/of 2022 1 16) (seq [jakso existing-jakso])
                   (LocalDate/of 2022 1 17) (seq [jakso])
                   (LocalDate/of 2022 1 18) (seq [jakso])
                   (LocalDate/of 2022 1 19) (seq [jakso])
                   (LocalDate/of 2022 1 20) (seq [jakso])}]
      (is (= (nh/add-to-jaksot-by-day-new jaksot-by-day jakso opiskeluoikeus)
             results)))))

(deftest test-handle-one-day
  (testing "Varmistaa, että handle-one-day toimii oikein."
    (let [jaksot (seq [{:hankkimistapa_id 1 :osa_aikaisuus 100}
                       {:hankkimistapa_id 2 :osa_aikaisuus 50}
                       {:hankkimistapa_id 3 :osa_aikaisuus 10}
                       {:hankkimistapa_id 4}])
          results {1 0.25
                   2 0.125
                   3 0.025
                   4 0.25}]
      (is (= (nh/handle-one-day jaksot) results)))))

(def test-compute-kestot-results (atom []))
(def test-compute-kestot-new-results (atom []))

(defn- do-rounding [values]
  (reduce-kv #(assoc %1 %2 (/ (Math/round (* %3 1000.0)) 1000.0)) {} values))

(defn- mock-get-opiskeluoikeus-catch-404 [oo-oid]
  (cond
    (= oo-oid "1.2.3.4") {:tila
                          {:opiskeluoikeusjaksot
                           [{:alku "2022-01-01" :tila {:koodiarvo "lasna"}}
                            {:alku "2022-01-30" :tila {:koodiarvo "loma"}}
                            {:alku "2022-02-10" :tila {:koodiarvo "lasna"}}]}}
    (= oo-oid "1.2.3.5") {:tila
                          {:opiskeluoikeusjaksot
                           [{:alku "2022-01-01" :tila {:koodiarvo "lasna"}}
                            {:alku "2022-02-02"
                             :tila {:koodiarvo "valiaikaisestikeskeytynyt"}}]}}
    :else {}))

(deftest test-compute-kestot
  (testing "Varmistaa, että compute-kestot laskee kestot oikein."
    (with-redefs
      [oph.heratepalvelu.external.ehoks/get-tyoelamajaksot-active-between
       (fn [oppija-oid start end]
         (reset! test-compute-kestot-results
                 (cons {:type "gtab" :start start :end end :oppija oppija-oid}
                       @test-compute-kestot-results))
         (seq [{:hankkimistapa_id 1
                :oppija_oid "4.4.4.4"
                :osa_aikaisuus 100
                :jakso_alkupvm "2022-01-03"
                :jakso_loppupvm "2022-02-05"
                :keskeytymisajanjaksot []
                :opiskeluoikeus_oid "1.2.3.1"}
               {:hankkimistapa_id 2
                :oppija_oid "4.4.4.4"
                :osa_aikaisuus 50
                :jakso_alkupvm "2022-02-07"
                :jakso_loppupvm "2022-04-04"
                :opiskeluoikeus_oid "1.2.3.2"}
               {:hankkimistapa_id 3
                :oppija_oid "4.4.4.4"
                :osa_aikaisuus 0
                :jakso_alkupvm "2022-01-31"
                :jakso_loppupvm "2022-02-20"
                :keskeytymisajanjaksot [{:alku "2022-02-16"
                                         :loppu "2022-02-18"}]
                :opiskeluoikeus_oid "1.2.3.3"}
               {:hankkimistapa_id 4
                :oppija_oid "4.4.4.4"
                :jakso_alkupvm "2022-01-17"
                :jakso_loppupvm "2022-02-20"
                :opiskeluoikeus_oid "1.2.3.4"}
               {:hankkimistapa_id 5
                :oppija_oid "4.4.4.4"
                :jakso_alkupvm "2022-01-01"
                :jakso_loppupvm "2022-04-01"
                :opiskeluoikeus_oid "1.2.3.5"}]))
       oph.heratepalvelu.external.koski/get-opiskeluoikeus-catch-404
       mock-get-opiskeluoikeus-catch-404]
      (let [jaksot [{:oppija_oid "4.4.4.4"
                     :jakso_alkupvm "2022-01-05"
                     :jakso_loppupvm "2022-02-28"}
                    {:oppija_oid "4.4.4.4"
                     :jakso_alkupvm "2022-01-10"
                     :jakso_loppupvm "2022-03-03"}]
            results {1 10.5
                     2 17.667
                     3 5.0
                     4 6.167
                     5 9.0}
            call-results [{:type "gtab"
                           :start "2022-01-05"
                           :end "2022-03-03"
                           :oppija "4.4.4.4"}]]
        (is (= (do-rounding (nh/compute-kestot jaksot)) results))
        (is (= (vec (reverse @test-compute-kestot-results)) call-results))))))

(deftest test-compute-kestot-new
  (testing "Varmistaa, että compute-kestot laskee kestot oikein."
    (with-redefs
      [oph.heratepalvelu.external.ehoks/get-tyoelamajaksot-active-between
       (fn [oppija-oid start end]
         (reset! test-compute-kestot-new-results
                 (cons {:type "gtab" :start start :end end :oppija oppija-oid}
                       @test-compute-kestot-new-results))
         (seq [{:hankkimistapa_id 1
                :oppija_oid "4.4.4.4"
                :osa_aikaisuus 100
                :jakso_alkupvm "2022-01-03"
                :jakso_loppupvm "2022-02-05"
                :keskeytymisajanjaksot []
                :opiskeluoikeus_oid "1.2.3.1"}
               {:hankkimistapa_id 2
                :oppija_oid "4.4.4.4"
                :osa_aikaisuus 50
                :jakso_alkupvm "2022-02-07"
                :jakso_loppupvm "2022-04-04"
                :opiskeluoikeus_oid "1.2.3.2"}
               {:hankkimistapa_id 3
                :oppija_oid "4.4.4.4"
                :osa_aikaisuus 0
                :jakso_alkupvm "2022-01-31"
                :jakso_loppupvm "2022-02-20"
                :keskeytymisajanjaksot [{:alku "2022-02-16"
                                         :loppu "2022-02-18"}]
                :opiskeluoikeus_oid "1.2.3.3"}
               {:hankkimistapa_id 4
                :oppija_oid "4.4.4.4"
                :jakso_alkupvm "2022-01-17"
                :jakso_loppupvm "2022-02-20"
                :opiskeluoikeus_oid "1.2.3.4"}
               {:hankkimistapa_id 5
                :oppija_oid "4.4.4.4"
                :jakso_alkupvm "2022-01-01"
                :jakso_loppupvm "2022-04-01"
                :opiskeluoikeus_oid "1.2.3.5"}]))
       oph.heratepalvelu.external.koski/get-opiskeluoikeus-catch-404
       mock-get-opiskeluoikeus-catch-404]
      (let [jaksot [{:oppija_oid "4.4.4.4"
                     :jakso_alkupvm "2022-01-05"
                     :jakso_loppupvm "2022-02-28"}
                    {:oppija_oid "4.4.4.4"
                     :jakso_alkupvm "2022-01-10"
                     :jakso_loppupvm "2022-03-03"}]
            expected {1 13.5
                      2 24.083
                      3 6.0
                      4 12.167
                      5 14.167}
            call-results [{:type "gtab"
                           :start "2022-01-05"
                           :end "2022-03-03"
                           :oppija "4.4.4.4"}]]
        (is (= (do-rounding (nh/compute-kestot-new jaksot)) expected))
        (is (= (vec (reverse @test-compute-kestot-new-results)) call-results))))))

(defn- mock-compute-kestot [jaksot] {(:oppija_oid (first jaksot)) (vec jaksot)})

(deftest test-group-jaksot-and-compute-kestot
  (testing "Varmistaa, että group-jaksot-and-compute-kestot toimii oikein."
    (with-redefs [oph.heratepalvelu.tep.niputusHandler/compute-kestot
                  mock-compute-kestot]
      (let [jaksot [{:oppija_oid "1234" :other_field "A"}
                    {:oppija_oid "5678" :other_field "B"}
                    {:oppija_oid "1234" :other_field "C"}
                    {:oppija_oid "8900" :other_field "D"}]
            results {"1234" [{:oppija_oid "1234" :other_field "C"}
                             {:oppija_oid "1234" :other_field "A"}]
                     "5678" [{:oppija_oid "5678" :other_field "B"}]
                     "8900" [{:oppija_oid "8900" :other_field "D"}]}]
        (is (= (nh/group-jaksot-and-compute-kestot jaksot) results))))))

(deftest test-query-jaksot
  (testing "Varmistaa, että query-jaksot toimii oikein."
    (with-redefs [environ.core/env {:jaksotunnus-table "table-name"}
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 3 3))
                  oph.heratepalvelu.db.dynamodb/query-items
                  (fn [query-params options table]
                    {:query-params query-params :options options :table table})]
      (let [nippu {:ohjaaja_ytunnus_kj_tutkinto "asdf"
                   :niputuspvm "2022-02-02"}
            results {:query-params
                     {:ohjaaja_ytunnus_kj_tutkinto [:eq [:s "asdf"]]
                      :niputuspvm [:eq [:s "2022-02-02"]]}
                     :options
                     {:index "niputusIndex"
                      :filter-expression
                      "#pvm >= :pvm AND attribute_exists(#tunnus)"
                      :expr-attr-names {"#pvm"    "viimeinen_vastauspvm"
                                        "#tunnus" "tunnus"}
                      :expr-attr-vals {":pvm" [:s "2022-03-03"]}}
                     :table "table-name"}]
        (is (= (nh/query-jaksot nippu) results))))))

(def trauj-results (atom []))

(defn- add-to-trauj-results [item]
  (reset! trauj-results (conj @trauj-results item)))

(defn- mock-trauj-query-jaksot [nippu]
  (add-to-trauj-results {:type "query-jaksot" :nippu nippu})
  [{:hankkimistapa_id 1} {:hankkimistapa_id 2} {:hankkimistapa_id 3}])

(defn- mock-trauj-group-jaksot-and-compute-kestot [jaksot]
  (add-to-trauj-results {:type "group-jaksot-and-compute-kestot"
                         :jaksot jaksot})
  {1 0.0 2 0.3 3 0.5})

(defn- mock-trauj-update-jakso [jakso updates]
  (add-to-trauj-results {:type "update-jakso" :jakso jakso :updates updates}))

(deftest test-retrieve-and-update-jaksot
  (testing "Varmistaa, että retrieve-and-update-jaksot toimii oikein."
    (with-redefs
      [oph.heratepalvelu.tep.niputusHandler/group-jaksot-and-compute-kestot
       mock-trauj-group-jaksot-and-compute-kestot
       oph.heratepalvelu.tep.niputusHandler/query-jaksot mock-trauj-query-jaksot
       oph.heratepalvelu.tep.tepCommon/update-jakso mock-trauj-update-jakso]
      (let [nippu {:mock-nippu-contents "asdf"}
            results [{:type "query-jaksot" :nippu {:mock-nippu-contents "asdf"}}
                     {:type "group-jaksot-and-compute-kestot"
                      :jaksot [{:hankkimistapa_id 1}
                               {:hankkimistapa_id 2}
                               {:hankkimistapa_id 3}]}
                     {:type "update-jakso"
                      :jakso {:hankkimistapa_id 1}
                      :updates {:kesto [:n 0]}}
                     {:type "update-jakso"
                      :jakso {:hankkimistapa_id 2}
                      :updates {:kesto [:n 0]}}
                     {:type "update-jakso"
                      :jakso {:hankkimistapa_id 3}
                      :updates {:kesto [:n 1]}}]
            updated-jaksot [{:hankkimistapa_id 1 :kesto 0}
                            {:hankkimistapa_id 2 :kesto 0}
                            {:hankkimistapa_id 3 :kesto 1}]]
        (is (= (nh/retrieve-and-update-jaksot nippu) updated-jaksot))
        (is (= @trauj-results results))))))

(def test-niputa-results (atom []))

(defn- add-to-test-niputa-results [data]
  (reset! test-niputa-results (cons data @test-niputa-results)))

(defn- mock-generate-uuid [] "test-uuid")

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

(defn- mock-retrieve-and-update-jaksot [nippu]
  (add-to-test-niputa-results {:type "mock-retrieve-and-update-jaksot"
                               :nippu nippu})
  (if (not= (:ohjaaja_ytunnus_kj_tutkinto nippu) "test-id-0")
    [{:tunnus "ABCDEF"
      :kesto 1.5
      :tyopaikan_nimi "Testi Työ Paikka"
      :viimeinen_vastauspvm "2022-02-02"}]
    []))

(deftest test-niputa
  (testing "Varmista, että niputa-funktio tekee oikeita kutsuja"
    (with-redefs
      [environ.core/env {:jaksotunnus-table "jaksotunnus-table-name"
                         :nippu-table "nippu-table-name"}
       oph.heratepalvelu.common/generate-uuid mock-generate-uuid
       oph.heratepalvelu.common/local-date-now (fn [] (LocalDate/of 2021 12 31))
       oph.heratepalvelu.common/rand-str (fn [_] "abcdef")
       oph.heratepalvelu.external.arvo/create-nippu-kyselylinkki
       mock-create-nippu-kyselylinkki
       oph.heratepalvelu.external.arvo/delete-nippukyselylinkki
       mock-delete-nippukyselylinkki
       oph.heratepalvelu.tep.niputusHandler/retrieve-and-update-jaksot
       mock-retrieve-and-update-jaksot
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
            results [{:type "mock-retrieve-and-update-jaksot"
                      :nippu test-nippu-0}
                     {:type "mock-update-nippu"
                      :nippu {:ohjaaja_ytunnus_kj_tutkinto "test-id-0"
                              :niputuspvm "2021-12-15"}
                      :updates
                      {:kasittelytila [:s (:ei-jaksoja c/kasittelytilat)]
                       :request_id [:s "test-uuid"]
                       :kasittelypvm [:s "2021-12-31"]}
                      :options {}}
                     {:type "mock-retrieve-and-update-jaksot"
                      :nippu test-nippu-1}
                     {:type "mock-create-nippu-kyselylinkki"
                      :niputus-request-body
                      {:tunniste "testi_tyo_paikka_2021-12-31_abcdef"
                       :koulutustoimija_oid "12345"
                       :tutkintotunnus "asdf"
                       :tyonantaja "123456-7"
                       :tyopaikka "Testityöpaikka"
                       :tunnukset [{:tunnus "ABCDEF"
                                    :tyopaikkajakson_kesto 1.5}]
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
                     {:type "mock-retrieve-and-update-jaksot"
                      :nippu test-nippu-2}
                     {:type "mock-create-nippu-kyselylinkki"
                      :niputus-request-body
                      {:tunniste "testi_tyo_paikka_2021-12-31_abcdef"
                       :koulutustoimija_oid "12111"
                       :tutkintotunnus "aaaa"
                       :tyonantaja "111111-1"
                       :tyopaikka nil
                       :tunnukset [{:tunnus "ABCDEF"
                                    :tyopaikkajakson_kesto 1.5}]
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
