(ns oph.heratepalvelu.amis.AMISSMSHandler-test
  "Testaa AMISSMSHandleriin liittyviä funktioita."
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.amis.AMISSMSHandler :as ash]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.elisa :as elisa]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(defn- mock-query-items [params options table] {:params params
                                                :options options
                                                :table table})

(deftest test-query-lahetettavat
  (testing "Varmistaa, että query-lahetettavat toimii oikein."
    (with-redefs [oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 3 3))
                  oph.heratepalvelu.db.dynamodb/query-items-with-expression mock-query-items
                  environ.core/env {:herate-table "herate-table-name"}]
      (is (= (ash/query-lahetettavat 20)
             {:params "#smstila = :tila AND #alku <= :pvm"
              :options
              {:index "smsIndex"
               :filter-expression "attribute_exists(#linkki)"
               :expr-attr-names {"#smstila" "sms-lahetystila", "#alku" "alkupvm", "#linkki" "kyselylinkki"}
               :expr-attr-vals {":tila" [:s "ei_lahetetty"], ":pvm" [:s "2022-03-03"]}
               :limit 20}
              :table "herate-table-name"})))))

(def results (atom []))

(defn- add-to-results [object] (reset! results (cons object @results)))

(defn- mock-query-lahetettavat [limit]
  (add-to-results {:type "mock-query-lahetettavat" :limit limit})
  [{:voimassa-loppupvm "2022-02-02"}
   {:voimassa-loppupvm "2022-04-04"
    :puhelinnumero "lkj12hl34kj1hl3412"}
   {:voimassa-loppupvm "2022-04-04"
    :puhelinnumero "12345"
    :kyselylinkki "kysely.linkki/123"}])

(defn- mock-update-herate [herate options]
  (add-to-results {:type "mock-update-herate" :herate herate :options options}))

(defn- mock-get-organisaatio [oppilaitos]
  (add-to-results {:type "mock-get-organisaatio"
                   :oppilaitos oppilaitos})
  {:nimi {:fi "Testilaitos" :en "Test Dept." :sv "Testanstalt"}})

(defn- mock-send-sms [numero body]
  (add-to-results {:type "mock-send-sms" :numero numero :body body})
  {:body {:messages {:12345 {:status "test-status" :converted "+358 12345"}}}})

(defn- mock-valid-number? [number] (= number "12345"))

(deftest test-handleAMISSMS
  (testing "Varmistaa, että -handleAMISSMS toimii oikein."
    (with-redefs [oph.heratepalvelu.amis.AMISCommon/update-herate
                  mock-update-herate
                  oph.heratepalvelu.amis.AMISSMSHandler/query-lahetettavat
                  mock-query-lahetettavat
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 3 3))
                  oph.heratepalvelu.common/valid-number? mock-valid-number?
                  oph.heratepalvelu.external.organisaatio/get-organisaatio
                  mock-get-organisaatio
                  oph.heratepalvelu.external.elisa/send-sms mock-send-sms]
      (let [expected [{:type "mock-query-lahetettavat" :limit 20}
                      {:type "mock-update-herate"
                       :herate {:voimassa-loppupvm "2022-02-02"}
                       :options {:sms-lahetyspvm [:s "2022-03-03"]
                                 :sms-lahetystila
                                 [:s (:vastausaika-loppunut c/kasittelytilat)]}}
                      {:type "mock-update-herate"
                       :herate {:voimassa-loppupvm "2022-04-04"
                                :puhelinnumero "lkj12hl34kj1hl3412"}
                       :options {:sms-lahetyspvm [:s "2022-03-03"]
                                 :sms-lahetystila
                                 [:s (:phone-invalid c/kasittelytilat)]}}
                      ;; TODO korjaa testi
                      {:type "mock-get-organisaatio"
                       :oppilaitos nil}
                      {:type "mock-send-sms"
                       :numero "12345"
                       :body (elisa/amis-msg-body
                               "kysely.linkki/123" "Testilaitos")}
                      {:type "mock-update-herate"
                       :herate {:voimassa-loppupvm "2022-04-04"
                                :puhelinnumero "12345"
                                :kyselylinkki "kysely.linkki/123"}
                       :options {:sms-lahetystila   [:s "test-status"]
                                 :sms-lahetyspvm    [:s "2022-03-03"]
                                 :lahetettynumeroon [:s "+358 12345"]}}]]
        (ash/-handleAMISSMS {}
                            (tu/mock-handler-event :scheduledherate)
                            (tu/mock-handler-context))
        (is (= expected (vec (reverse @results))))))))
