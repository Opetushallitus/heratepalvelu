(ns oph.heratepalvelu.tpk.tpkArvoCallHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.test-util :as tu]
            [oph.heratepalvelu.tpk.tpkArvoCallHandler :as tpka])
  (:import (java.time LocalDate)))

(deftest test-do-scan
  (testing "Varmistaa, että do-scan tekee oikeita kutsuja"
    (with-redefs [environ.core/env {:tpk-nippu-table "tpk-nippu-table-name"}
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 2 2))
                  oph.heratepalvelu.db.dynamodb/scan
                  (fn [options table] (assoc options :table table))]
      (is (= (tpka/do-scan "asdf")
             {:filter-expression
              "attribute_not_exists(#linkki) AND #kausi = :kausi"
              :exclusive-start-key "asdf"
              :expr-attr-names {"#kausi"  "tiedonkeruu-alkupvm"
                                "#linkki" "kyselylinkki"}
              :expr-attr-vals {":kausi" [:s "2021-07-01"]}
              :table "tpk-nippu-table-name"})))))

(deftest test-make-arvo-request
  (testing "Varmistaa, että make-arvo-request tekee kutsuja oikein"
    (with-redefs [oph.heratepalvelu.external.arvo/build-tpk-request-body
                  (fn [nippu] {:test-field-body (:test-field-nippu nippu)
                               :request-id (:request-id nippu)})
                  oph.heratepalvelu.external.arvo/create-tpk-kyselylinkki
                  (fn [body] {:test-field (:test-field-body body)
                              :request-id (:request-id body)
                              :kyselylinkki "kysely.linkki/123"
                              :tunnus "QWERTY"
                              :voimassa-loppupvm "2021-12-12"})]
      (let [nippu {:test-field-nippu "test-field"
                   :nippu-id "test-id"}
            expected {:test-field "test-field"
                      :request-id "asdf"
                      :kyselylinkki "kysely.linkki/123"
                      :tunnus "QWERTY"
                      :voimassa-loppupvm "2021-12-12"}]
        (is (= (tpka/make-arvo-request nippu "asdf") expected))))))

(deftest test-handleTpkArvoCalls
  (testing "Varmistaa, että -handleTpkArvoCalls tekee oikeita kutsuja"
    (let [results (atom [])]
      (with-redefs [environ.core/env {:tpk-nippu-table "tpk-nippu-table-name"}
                    oph.heratepalvelu.common/generate-uuid (fn [] "test-uuid")
                    oph.heratepalvelu.db.dynamodb/update-item
                    (fn [key-conds options table]
                      (reset! results (cons {:key-conds key-conds
                                             :options options
                                             :table table}
                                            @results)))
                    oph.heratepalvelu.tpk.tpkArvoCallHandler/do-scan
                    (fn [] {:items [{:nippu-id "test-nippu-id-1"
                                     :tiedonkeruu-alkupvm "2021-07-01"}
                                    {:nippu-id "test-nippu-id-2"
                                     :tiedonkeruu-alkupvm "2021-07-01"}]})
                    oph.heratepalvelu.tpk.tpkArvoCallHandler/make-arvo-request
                    (fn [nippu request-id]
                      (reset! results (cons (assoc nippu :request-id request-id)
                                            @results))
                      (when (= (:nippu-id nippu) "test-nippu-id-1")
                        {:kysely_linkki "kysely.linkki/ABCD"
                         :tunnus "ABCD"
                         :voimassa_loppupvm "2022-02-28"}))]
        (tpka/-handleTpkArvoCalls {}
                                  (tu/mock-handler-event :scheduledherate)
                                  (tu/mock-handler-context 40000))
        (is (= (vec (reverse @results))
               [{:nippu-id "test-nippu-id-1"
                 :tiedonkeruu-alkupvm "2021-07-01"
                 :request-id "test-uuid"}
                {:key-conds {:nippu-id [:s "test-nippu-id-1"]}
                 :options {:update-expr (str "SET #linkki = :linkki, "
                                             "#tunnus = :tunnus, #pvm = :pvm, "
                                             "#req = :req")
                           :expr-attr-names {"#linkki" "kyselylinkki"
                                             "#tunnus" "tunnus"
                                             "#pvm"    "voimassa-loppupvm"
                                             "#req"    "request-id"}
                           :expr-attr-vals {":linkki" [:s "kysely.linkki/ABCD"]
                                            ":tunnus" [:s "ABCD"]
                                            ":pvm"    [:s "2022-02-28"]
                                            ":req"    [:s "test-uuid"]}}
                 :table "tpk-nippu-table-name"}
                {:nippu-id "test-nippu-id-2"
                 :tiedonkeruu-alkupvm "2021-07-01"
                 :request-id "test-uuid"}]))))))
