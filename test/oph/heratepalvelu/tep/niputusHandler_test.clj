(ns oph.heratepalvelu.tep.niputusHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.tep.niputusHandler :as nh]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

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
             (= "#pvm >= :pvm" (:filter-expression options))
             (= "viimeinen_vastauspvm" (get (:expr-attr-names options) "#pvm"))
             (= :s (first (get (:expr-attr-vals options) ":pvm")))
             (= "jaksotunnus-table-name" table))
    (add-to-test-niputa-results {:type "mock-niputa-query-items"
                                 :pvm (second
                                        (get (:expr-attr-vals options) ":pvm"))
                                 :ohjaaja_ytunnus_kj_tutkinto
                                 (second (second (:ohjaaja_ytunnus_kj_tutkinto
                                                   query-params)))
                                 :niputuspvm
                                 (second (second (:niputuspvm query-params)))})
    (if (not= (second (second (:ohjaaja_ytunnus_kj_tutkinto query-params)))
              "test-id-0")
      [{:tunnus "ABCDEF"
        :tyopaikan_nimi "Testi Työ Paikka"
        :viimeinen_vastauspvm "2022-02-02"}]
      [])))

(defn- mock-niputa-update-item [query-params options table]
  (when (and (= :s (first (:ohjaaja_ytunnus_kj_tutkinto query-params)))
             (= :s (first (:niputuspvm query-params)))
             (= "kasittelytila" (get (:expr-attr-names options) "#tila"))
             (= "request_id" (get (:expr-attr-names options) "#req"))
             (= "kasittelypvm" (get (:expr-attr-names options) "#pvm"))
             (or (= "reason" (get (:expr-attr-names options) "#reason"))
                 (nil? (get (:expr-attr-names options) "#reason")))
             (or (= "voimassaloppupvm"
                    (get (:expr-attr-names options) "#voimassa"))
                 (nil? (get (:expr-attr-names options) "#voimassa")))
             (or (= "kyselylinkki" (get (:expr-attr-names options) "#linkki"))
                 (nil? (get (:expr-attr-names options) "#linkki")))
             (= :s (first (get (:expr-attr-vals options) ":tila")))
             (= :s (first (get (:expr-attr-vals options) ":req")))
             (or (= :s (first (get (:expr-attr-vals options) ":reason")))
                 (nil? (get (:expr-attr-vals options) ":reason")))
             (or (= :s (first (get (:expr-attr-vals options) ":voimassa")))
                 (nil? (get (:expr-attr-vals options) ":voimassa")))
             (or (= :s (first (get (:expr-attr-vals options) ":linkki")))
                 (nil? (get (:expr-attr-vals options) ":linkki")))
             (= :s (first (get (:expr-attr-vals options) ":pvm")))
             (= "2021-12-31" (second (get (:expr-attr-vals options) ":pvm")))
             (= "nippu-table-name" table))
    (add-to-test-niputa-results {:type "mock-niputa-update-item"
                                 :ohjaaja_ytunnus_kj_tutkinto
                                 (second
                                   (:ohjaaja_ytunnus_kj_tutkinto query-params))
                                 :niputuspvm (second (:niputuspvm query-params))
                                 :tila (second (get (:expr-attr-vals options)
                                                    ":tila"))
                                 :req (second (get (:expr-attr-vals options)
                                                   ":req"))
                                 :reason (second (get (:expr-attr-vals options)
                                                      ":reason"))
                                 :voimassa (second
                                             (get (:expr-attr-vals options)
                                                  ":voimassa"))
                                 :linkki (second (get (:expr-attr-vals options)
                                                      ":linkki"))
                                 :update-expr (:update-expr options)
                                 :cond-expr (:cond-expr options)})))

(defn- mock-create-nippu-kyselylinkki [niputus-request-body]
  (add-to-test-niputa-results {:type "mock-create-nippu-kyselylinkki"
                               :niputus-request-body niputus-request-body})
  {:nippulinkki (when (= "123456-7" (:tyonantaja niputus-request-body))
                  "kysely.linkki/132")
   :voimassa_loppupvm "2021-12-17"})

(defn- mock-delete-nippukyselylinkki [tunniste]
  (add-to-test-niputa-results {:type "mock-delete-nippukyselylinkki"
                               :tunniste tunniste}))

(deftest test-niputa
  (testing "Varmista, että niputa-funktio tekee oikeita kutsuja"
    (with-redefs
      [environ.core/env {:jaksotunnus-table "jaksotunnus-table-name"
                         :nippu-table "nippu-table-name"}
       oph.heratepalvelu.common/generate-uuid mock-generate-uuid
       oph.heratepalvelu.common/local-date-now (fn [] (LocalDate/of 2021 12 31))
       oph.heratepalvelu.common/rand-str (fn [x] "abcdef")
       oph.heratepalvelu.db.dynamodb/query-items mock-niputa-query-items
       oph.heratepalvelu.db.dynamodb/update-item mock-niputa-update-item
       oph.heratepalvelu.external.arvo/create-nippu-kyselylinkki
       mock-create-nippu-kyselylinkki
       oph.heratepalvelu.external.arvo/delete-nippukyselylinkki
       mock-delete-nippukyselylinkki]
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
                     {:type "mock-niputa-update-item"
                      :ohjaaja_ytunnus_kj_tutkinto "test-id-0"
                      :niputuspvm "2021-12-15"
                      :update-expr "SET #tila = :tila, #pvm = :pvm, #req = :req"
                      :cond-expr nil
                      :tila "ei-jaksoja"
                      :req "test-uuid"
                      :reason nil
                      :voimassa nil
                      :linkki nil}
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
                     {:type "mock-niputa-update-item"
                      :ohjaaja_ytunnus_kj_tutkinto "test-id-1"
                      :niputuspvm "2021-12-15"
                      :update-expr (str "SET #tila = :tila, #pvm = :pvm, "
                                        "#linkki = :linkki, "
                                        "#voimassa = :voimassa, #req = :req")
                      :cond-expr "attribute_not_exists(kyselylinkki)"
                      :tila (:ei-lahetetty c/kasittelytilat)
                      :linkki "kysely.linkki/132"
                      :voimassa "2021-12-17"
                      :req "test-uuid"
                      :reason nil}
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
                     {:type "mock-niputa-update-item"
                      :ohjaaja_ytunnus_kj_tutkinto "test-id-2"
                      :niputuspvm "2021-12-15"
                      :update-expr (str "SET #tila = :tila, #pvm = :pvm, "
                                        "#reason = :reason, #req = :req")
                      :cond-expr nil
                      :tila "niputusvirhe"
                      :linkki nil
                      :voimassa nil
                      :req "test-uuid"
                      :reason "no reason in response"}]]
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
