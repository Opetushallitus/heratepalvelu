(ns oph.heratepalvelu.tep.tepSmsHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.elisa :as elisa]
            [oph.heratepalvelu.tep.tepSmsHandler :as sh]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(deftest valid-number-test
  (testing "Funktio valid-number? tunnistaa oikeita ja virheellisiä puhelinnumeroja"
    (let [fi-phone-number "040 654 3210"
          fi-phone-number-intl-fmt "040 654 3210"
          intl-phone-number "+1 517 987 5432"
          junk-invalid "laksj fdaiu fd098098asdf"
          unicode-invalid "+358 40 987 6543à"]
      (is (sh/valid-number? fi-phone-number))
      (is (sh/valid-number? fi-phone-number-intl-fmt))
      (is (sh/valid-number? intl-phone-number))
      (is (not (sh/valid-number? junk-invalid)))
      (is (not (sh/valid-number? unicode-invalid))))))

(deftest client-error-test
  (testing "Funktio client-error? erottaa client erroreja muista HTTP-statuksista"
    (let [client-error (ex-info "File not found" {:status 404})
          server-error (ex-info "Internal server error" {:status 503})]
      (is (sh/client-error? client-error))
      (is (not (sh/client-error? server-error))))))

(deftest update-arvo-obj-sms-test
  (testing "Funktio update-arvo-obj-sms luo oikean objektin patch-nippulinkkiin"
    (let [success-new-loppupvm {:tila (:success c/kasittelytilat)
                                :voimassa_loppupvm "2021-09-09"}
          success              {:tila (:success c/kasittelytilat)}
          failure              {:tila (:failure c/kasittelytilat)}]
      (is (= (sh/update-arvo-obj-sms "CREATED" "2021-09-09") success-new-loppupvm))
      (is (= (sh/update-arvo-obj-sms "CREATED" nil) success))
      (is (= (sh/update-arvo-obj-sms "asdfads" nil) failure)))))

(def test-update-status-to-db-results (atom {}))

(defn- mock-update-status-to-db-update-item [query-params options table]
  (when (and (= :s (first (:ohjaaja_ytunnus_kj_tutkinto query-params)))
             (= :s (first (:niputuspvm query-params)))
             (= (:update-expr options)
                (str "SET #sms_kasittelytila = :sms_kasittelytila, "
                     "#sms_lahetyspvm = :sms_lahetyspvm, "
                     "#sms_muistutukset = :sms_muistutukset, "
                     "#lahetettynumeroon = :lahetettynumeroon, "
                     "#loppupvm = :loppupvm"))
             (= "sms_kasittelytila"
                (get (:expr-attr-names options) "#sms_kasittelytila"))
             (= "sms_lahetyspvm"
                (get (:expr-attr-names options) "#sms_lahetyspvm"))
             (= "sms_muistutukset"
                (get (:expr-attr-names options) "#sms_muistutukset"))
             (= "lahetettynumeroon"
                (get (:expr-attr-names options) "#lahetettynumeroon"))
             (= "voimassaloppupvm" (get (:expr-attr-names options) "#loppupvm"))
             (= :s (first (get (:expr-attr-vals options) ":sms_kasittelytila")))
             (= :s (first (get (:expr-attr-vals options) ":sms_lahetyspvm")))
             (= :n (first (get (:expr-attr-vals options) ":sms_muistutukset")))
             (= 0 (second (get (:expr-attr-vals options) ":sms_muistutukset")))
             (= :s (first (get (:expr-attr-vals options) ":lahetettynumeroon")))
             (= :s (first (get (:expr-attr-vals options) ":loppupvm")))
             (= table "nippu-table-name"))
    (reset! test-update-status-to-db-results
            {:ohjaaja_ytunnus_kj_tutkinto
             (second (:ohjaaja_ytunnus_kj_tutkinto query-params))
             :niputuspvm (second (:niputuspvm query-params))
             :sms_kasittelytila
             (second (get (:expr-attr-vals options) ":sms_kasittelytila"))
             :sms_lahetyspvm
             (second (get (:expr-attr-vals options) ":sms_lahetyspvm"))
             :lahetettynumeroon
             (second (get (:expr-attr-vals options) ":lahetettynumeroon"))
             :loppupvm (second (get (:expr-attr-vals options) ":loppupvm"))})))

(deftest test-update-status-to-db
  (testing "Varmista, että update-status-to-db kutsuu update-item oikein"
    (with-redefs [environ.core/env {:nippu-table "nippu-table-name"}
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2021 12 20))
                  oph.heratepalvelu.db.dynamodb/update-item
                  mock-update-status-to-db-update-item]
      (let [status (:success c/kasittelytilat)
            puhelinnumero "+358 12 345 6789"
            nippu {:ohjaaja_ytunnus_kj_tutkinto "test-id"
                   :niputuspvm "2021-12-15"
                   :voimassaloppupvm "2021-12-30"}
            new-loppupvm "2022-03-04"
            results1 {:ohjaaja_ytunnus_kj_tutkinto "test-id"
                      :niputuspvm "2021-12-15"
                      :sms_kasittelytila (:success c/kasittelytilat)
                      :sms_lahetyspvm "2021-12-20"
                      :lahetettynumeroon "+358 12 345 6789"
                      :loppupvm "2022-03-04"}
            results2 {:ohjaaja_ytunnus_kj_tutkinto "test-id"
                      :niputuspvm "2021-12-15"
                      :sms_kasittelytila (:success c/kasittelytilat)
                      :sms_lahetyspvm "2021-12-20"
                      :lahetettynumeroon "+358 12 345 6789"
                      :loppupvm "2021-12-30"}]
        (sh/update-status-to-db status puhelinnumero nippu new-loppupvm)
        (is (= results1 @test-update-status-to-db-results))
        (sh/update-status-to-db status puhelinnumero nippu nil)
        (is (= results2 @test-update-status-to-db-results))))))

(def test-ohjaaja-puhnro-results (atom []))

(defn- add-to-test-ohjaaja-puhnro-results [data]
  (reset! test-ohjaaja-puhnro-results (cons data @test-ohjaaja-puhnro-results)))

(defn- reset-test-ohjaaja-puhnro-results []
  (reset! test-ohjaaja-puhnro-results []))

(defn- mock-ohjaaja-puhnro-update-item [query-params options table]
  (when (and (= :s (first (:ohjaaja_ytunnus_kj_tutkinto query-params)))
             (= :s (first (:niputuspvm query-params)))
             (= "sms_kasittelytila" (get (:expr-attr-names options)
                                         "#sms_kasittelytila"))
             (= :s (first (get (:expr-attr-vals options) ":sms_kasittelytila")))
             (or (= :s (first (get (:expr-attr-vals options)
                                   ":lahetettynumeroon")))
                 (nil? (get (:expr-attr-vals options) ":lahetettynumeroon")))
             (= "nippu-table-name" table))
    (add-to-test-ohjaaja-puhnro-results
      {:type "mock-ohjaaja-puhnro-update-item"
       :ohjaaja_ytunnus_kj_tutkinto
       (second (:ohjaaja_ytunnus_kj_tutkinto query-params))
       :niputuspvm (second (:niputuspvm query-params))
       :update-expr (:update-expr options)
       :lahetettynumeroon-attr-name
       (get (:expr-attr-names options) "#lahetettynumeroon")
       :sms_kasittelytila
       (second (get (:expr-attr-vals options) ":sms_kasittelytila"))
       :lahetettynumeroon
       (second (get (:expr-attr-vals options) ":lahetettynumeroon"))})))

(defn- mock-ohjaaja-puhnro-patch-nippulinkki [kyselylinkki data]
  (add-to-test-ohjaaja-puhnro-results
    {:type "mock-ohjaaja-puhnro-patch-nippulinkki"
     :kyselylinkki kyselylinkki
     :data data}))

(deftest test-ohjaaja-puhnro
  (testing "Ohjaaja-puhnro kutsuu update-item ja patch-nippulinkki oikein"
    (with-redefs [environ.core/env {:nippu-table "nippu-table-name"}
                  oph.heratepalvelu.db.dynamodb/update-item
                  mock-ohjaaja-puhnro-update-item
                  oph.heratepalvelu.external.arvo/patch-nippulinkki
                  mock-ohjaaja-puhnro-patch-nippulinkki]
      (let [jaksot-one-valid-number [{:ohjaaja_puhelinnumero "+358401234567"}
                                     {:ohjaaja_puhelinnumero "+358401234567"}]
            jaksot-one-invalid-number [{:ohjaaja_puhelinnumero "1234"}
                                       {:ohjaaja_puhelinnumero "1234"}]
            jaksot-no-number [{}]
            jaksot-mismatch [{:ohjaaja_puhelinnumero "+358401234567"}
                             {:ohjaaja_puhelinnumero "+358407654321"}]
            nippu-base {:ohjaaja_ytunnus_kj_tutkinto "test-id"
                        :niputuspvm "2021-12-15"
                        :kyselylinkki "kysely.linkki/123"}
            nippu-good-email
            (assoc nippu-base :kasittelytila (:ei-lahetetty c/kasittelytilat))
            nippu-email-mismatch
            (assoc nippu-base :kasittelytila (:email-mismatch c/kasittelytilat))
            nippu-no-email
            (assoc nippu-base :kasittelytila (:no-email c/kasittelytilat))]
        (is (= "+358401234567"
               (sh/ohjaaja-puhnro nippu-good-email jaksot-one-valid-number)))
        (is (= (vec (reverse @test-ohjaaja-puhnro-results)) []))
        (reset-test-ohjaaja-puhnro-results)
        (is (nil? (sh/ohjaaja-puhnro nippu-good-email
                                     jaksot-one-invalid-number)))
        (is (= (vec (reverse @test-ohjaaja-puhnro-results))
               [{:type "mock-ohjaaja-puhnro-update-item"
                 :ohjaaja_ytunnus_kj_tutkinto "test-id"
                 :niputuspvm "2021-12-15"
                 :update-expr
                 (str "SET #sms_kasittelytila = :sms_kasittelytila, "
                      "#lahetettynumeroon = :lahetettynumeroon")
                 :lahetettynumeroon-attr-name "lahetettynumeroon"
                 :sms_kasittelytila (:phone-invalid c/kasittelytilat)
                 :lahetettynumeroon "1234"}]))
        (reset-test-ohjaaja-puhnro-results)
        (is (nil? (sh/ohjaaja-puhnro nippu-email-mismatch
                                     jaksot-one-invalid-number)))
        (is (= (vec (reverse @test-ohjaaja-puhnro-results))
               [{:type "mock-ohjaaja-puhnro-update-item"
                 :ohjaaja_ytunnus_kj_tutkinto "test-id"
                 :niputuspvm "2021-12-15"
                 :update-expr
                 (str "SET #sms_kasittelytila = :sms_kasittelytila, "
                      "#lahetettynumeroon = :lahetettynumeroon")
                 :lahetettynumeroon-attr-name "lahetettynumeroon"
                 :sms_kasittelytila (:phone-invalid c/kasittelytilat)
                 :lahetettynumeroon "1234"}
                {:type "mock-ohjaaja-puhnro-patch-nippulinkki"
                 :kyselylinkki "kysely.linkki/123"
                 :data {:tila (:ei-yhteystietoja c/kasittelytilat)}}]))
        (reset-test-ohjaaja-puhnro-results)
        (is (nil? (sh/ohjaaja-puhnro nippu-no-email jaksot-one-invalid-number)))
        (is (= (vec (reverse @test-ohjaaja-puhnro-results))
               [{:type "mock-ohjaaja-puhnro-update-item"
                 :ohjaaja_ytunnus_kj_tutkinto "test-id"
                 :niputuspvm "2021-12-15"
                 :update-expr
                 (str "SET #sms_kasittelytila = :sms_kasittelytila, "
                      "#lahetettynumeroon = :lahetettynumeroon")
                 :lahetettynumeroon-attr-name "lahetettynumeroon"
                 :sms_kasittelytila (:phone-invalid c/kasittelytilat)
                 :lahetettynumeroon "1234"}
                {:type "mock-ohjaaja-puhnro-patch-nippulinkki"
                 :kyselylinkki "kysely.linkki/123"
                 :data {:tila (:ei-yhteystietoja c/kasittelytilat)}}]))
        (reset-test-ohjaaja-puhnro-results)
        (is (nil? (sh/ohjaaja-puhnro nippu-good-email jaksot-no-number)))
        (is (= (vec (reverse @test-ohjaaja-puhnro-results))
               [{:type "mock-ohjaaja-puhnro-update-item"
                 :ohjaaja_ytunnus_kj_tutkinto "test-id"
                 :niputuspvm "2021-12-15"
                 :update-expr "SET #sms_kasittelytila = :sms_kasittelytila"
                 :lahetettynumeroon-attr-name nil
                 :sms_kasittelytila (:no-phone c/kasittelytilat)
                 :lahetettynumeroon nil}]))
        (reset-test-ohjaaja-puhnro-results)
        (is (nil? (sh/ohjaaja-puhnro nippu-email-mismatch jaksot-mismatch)))
        (is (= (vec (reverse @test-ohjaaja-puhnro-results))
               [{:type "mock-ohjaaja-puhnro-update-item"
                 :ohjaaja_ytunnus_kj_tutkinto "test-id"
                 :niputuspvm "2021-12-15"
                 :update-expr "SET #sms_kasittelytila = :sms_kasittelytila"
                 :lahetettynumeroon-attr-name nil
                 :sms_kasittelytila (:phone-mismatch c/kasittelytilat)
                 :lahetettynumeroon nil}
                {:type "mock-ohjaaja-puhnro-patch-nippulinkki"
                 :kyselylinkki "kysely.linkki/123"
                 :data {:tila (:ei-yhteystietoja c/kasittelytilat)}}
                ]))
        (reset-test-ohjaaja-puhnro-results)
        (is (nil? (sh/ohjaaja-puhnro nippu-no-email jaksot-no-number)))
        (is (= (vec (reverse @test-ohjaaja-puhnro-results))
               [{:type "mock-ohjaaja-puhnro-update-item"
                 :ohjaaja_ytunnus_kj_tutkinto "test-id"
                 :niputuspvm "2021-12-15"
                 :update-expr "SET #sms_kasittelytila = :sms_kasittelytila"
                 :lahetettynumeroon-attr-name nil
                 :sms_kasittelytila (:no-phone c/kasittelytilat)
                 :lahetettynumeroon nil}
                {:type "mock-ohjaaja-puhnro-patch-nippulinkki"
                 :kyselylinkki "kysely.linkki/123"
                 :data {:tila (:ei-yhteystietoja c/kasittelytilat)}}]))))))

(def test-update-vastausaika-loppunut-to-db-results (atom {}))

(defn- mock-update-vastausaika-loppunut-to-db-update-item [query-params
                                                           options
                                                           table]
  (when (and (= :s (first (:ohjaaja_ytunnus_kj_tutkinto query-params)))
             (= :s (first (:niputuspvm query-params)))
             (= (str "SET #sms_kasittelytila = :sms_kasittelytila, "
                     "#sms_lahetyspvm = :sms_lahetyspvm")
                (:update-expr options))
             (= "sms_kasittelytila" (get (:expr-attr-names options)
                                         "#sms_kasittelytila"))
             (= "sms_lahetyspvm" (get (:expr-attr-names options)
                                      "#sms_lahetyspvm"))
             (= :s (first (get (:expr-attr-vals options) ":sms_kasittelytila")))
             (= (:vastausaika-loppunut c/kasittelytilat)
                (second (get (:expr-attr-vals options) ":sms_kasittelytila")))
             (= :s (first (get (:expr-attr-vals options) ":sms_lahetyspvm")))
             (= "2021-12-15"
                (second (get (:expr-attr-vals options) ":sms_lahetyspvm")))
             (= "nippu-table-name" table))
    (reset! test-update-vastausaika-loppunut-to-db-results
            {:ohjaaja_ytunnus_kj_tutkinto
             (second (:ohjaaja_ytunnus_kj_tutkinto query-params))
             :niputuspvm (second (:niputuspvm query-params))})))

(deftest test-update-vastausaika-loppunut-to-db
  (testing "Varmista, että update-vastausaika-loppunut-to-db toimii oikein"
    (with-redefs [environ.core/env {:nippu-table "nippu-table-name"}
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2021 12 15))
                  oph.heratepalvelu.db.dynamodb/update-item
                  mock-update-vastausaika-loppunut-to-db-update-item]
      (let [nippu {:ohjaaja_ytunnus_kj_tutkinto "test-id"
                   :niputuspvm "2021-12-10"}
            results {:ohjaaja_ytunnus_kj_tutkinto "test-id"
                     :niputuspvm "2021-12-10"}]
        (sh/update-vastausaika-loppunut-to-db nippu)
        (is (= results @test-update-vastausaika-loppunut-to-db-results))))))

(def test-query-lahetettavat-results (atom {}))

(defn- mock-query-lahetettavat-query-items [query-params options table]
  (when (and (= :eq (first (:sms_kasittelytila query-params)))
             (= :s (first (second (:sms_kasittelytila query-params))))
             (= (:ei-lahetetty c/kasittelytilat)
                (second (second (:sms_kasittelytila query-params))))
             (= :le (first (:niputuspvm query-params)))
             (= :s (first (second (:niputuspvm query-params))))
             (= "smsIndex" (:index options))
             (= "nippu-table-name" table))
    (reset! test-query-lahetettavat-results
            {:limit (:limit options)
             :niputuspvm (second (second (:niputuspvm query-params)))})))

(deftest test-query-lahetettavat
  (testing "Varmista, että query-lahetettavat kutsuu query-items oikein"
    (with-redefs [environ.core/env {:nippu-table "nippu-table-name"}
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2021 12 15))
                  oph.heratepalvelu.db.dynamodb/query-items
                  mock-query-lahetettavat-query-items]
      (sh/query-lahetettavat 10)
      (is (= @test-query-lahetettavat-results {:limit 10
                                               :niputuspvm "2021-12-15"})))))

(def test-handleTepSmsSending-results (atom []))

(defn- add-to-test-handleTepSmsSending-results [data]
  (reset! test-handleTepSmsSending-results
          (cons data @test-handleTepSmsSending-results)))

(defn- mock-get-jaksot-for-nippu [nippu]
  (add-to-test-handleTepSmsSending-results {:type "mock-get-jaksot-for-nippu"
                                            :nippu nippu})
  [{:oppilaitos "1234"
    :ohjaaja_puhelinnumero (if (= (:ohjaaja_ytunnus_kj_tutkinto nippu)
                                  "test-id-1")
                             "+358401234567"
                             "0401234567")}])

(defn- mock-handleTepSmsSending-get-organisaatio [oppilaitos]
  (add-to-test-handleTepSmsSending-results
    {:type "mock-handleTepSmsSending-get-organisaatio"
     :oppilaitos oppilaitos})
  {:nimi {:fi "Testilaitos"
          :en "Test Dept."
          :sv "Testanstalt"}})

(defn- mock-handleTepSmsSending-send-tep-sms [puhelinnumero body]
  (add-to-test-handleTepSmsSending-results
    {:type "mock-handleTepSmsSending-send-tep-sms"
     :puhelinnumero puhelinnumero
     :body body})
  {:body {:messages {(keyword puhelinnumero) (if (= "0401234567" puhelinnumero)
                                               {:status "mock-lahetys"
                                                :converted "+358401234567"}
                                               {:status "mock-lahetys"})}}})

(defn- mock-handleTepSmsSending-update-status-to-db [status
                                                     puhelinnumero
                                                     nippu
                                                     new-loppupvm]
  (add-to-test-handleTepSmsSending-results
    {:type "mock-handleTepSmsSending-update-status-to-db"
     :status status
     :puhelinnumero puhelinnumero
     :nippu nippu
     :new-loppupvm new-loppupvm}))

(defn- mock-handleTepSmsSending-patch-nippulinkki [kyselylinkki data]
  (add-to-test-handleTepSmsSending-results
    {:type "mock-handleTepSmsSending-patch-nippulinkki"
     :kyselylinkki kyselylinkki
     :data data}))

(defn- mock-handleTepSmsSending-update-vastausaika-loppunut-to-db [nippu]
  (add-to-test-handleTepSmsSending-results
    {:type "mock-handleTepSmsSending-update-vastausaika-loppunut-to-db"
     :nippu nippu}))

(defn- mock-query-lahetettavat [limit]
  (add-to-test-handleTepSmsSending-results {:type "mock-query-lahetettavat"
                                            :limit limit})
  [{:ohjaaja_ytunnus_kj_tutkinto "test-id-0"
    :niputuspvm "2021-12-15"
    :voimassaloppupvm "2021-12-20"
    :sms_kasittelytila (:ei-lahetetty c/kasittelytilat)
    :kyselylinkki "kysely.linkki/0"}
   {:ohjaaja_ytunnus_kj_tutkinto "test-id-1"
    :niputuspvm "2021-12-15"
    :voimassaloppupvm "2021-12-30"
    :sms_kasittelytila (:ei-lahetetty c/kasittelytilat)
    :kyselylinkki "kysely.linkki/1"}
   {:ohjaaja_ytunnus_kj_tutkinto "test-id-2"
    :niputuspvm "2021-12-15"
    :voimassaloppupvm "2021-12-31"
    :sms_kasittelytila (:ei-lahetetty c/kasittelytilat)
    :kyselylinkki "kysely.linkki/2"}])

(defn- mock-has-time-to-answer? [voimassaloppupvm]
  (< 0 (compare voimassaloppupvm "2021-12-20")))

(deftest test-handleTepSmsSending
  (testing "Varmista, että -handleTepSmsSending kutsuu funktioita oikein"
    (with-redefs 
      [environ.core/env {:jaksotunnus-table "jaksotunnus-table-name"}
       oph.heratepalvelu.common/has-time-to-answer? mock-has-time-to-answer?
       oph.heratepalvelu.common/local-date-now (fn [] (LocalDate/of 2021 12 20))
       oph.heratepalvelu.external.arvo/patch-nippulinkki
       mock-handleTepSmsSending-patch-nippulinkki
       oph.heratepalvelu.external.elisa/send-tep-sms
       mock-handleTepSmsSending-send-tep-sms
       oph.heratepalvelu.external.organisaatio/get-organisaatio
       mock-handleTepSmsSending-get-organisaatio
       oph.heratepalvelu.tep.tepCommon/get-jaksot-for-nippu
       mock-get-jaksot-for-nippu
       oph.heratepalvelu.tep.tepSmsHandler/query-lahetettavat
       mock-query-lahetettavat
       oph.heratepalvelu.tep.tepSmsHandler/update-status-to-db
       mock-handleTepSmsSending-update-status-to-db
       oph.heratepalvelu.tep.tepSmsHandler/update-vastausaika-loppunut-to-db
       mock-handleTepSmsSending-update-vastausaika-loppunut-to-db]
      (let [event (tu/mock-handler-event :scheduledherate)
            context (tu/mock-handler-context)
            results [{:type "mock-query-lahetettavat"
                      :limit 20}
                     {:type (str "mock-handleTepSmsSending-update-vastausaika"
                                 "-loppunut-to-db")
                      :nippu {:ohjaaja_ytunnus_kj_tutkinto "test-id-0"
                              :niputuspvm "2021-12-15"
                              :voimassaloppupvm "2021-12-20"
                              :sms_kasittelytila (:ei-lahetetty
                                                   c/kasittelytilat)
                              :kyselylinkki "kysely.linkki/0"}}
                     {:type "mock-get-jaksot-for-nippu"
                      :nippu {:ohjaaja_ytunnus_kj_tutkinto "test-id-1"
                              :niputuspvm "2021-12-15"
                              :voimassaloppupvm "2021-12-30"
                              :sms_kasittelytila (:ei-lahetetty
                                                   c/kasittelytilat)
                              :kyselylinkki "kysely.linkki/1"}}
                     {:type "mock-handleTepSmsSending-get-organisaatio"
                      :oppilaitos "1234"}
                     {:type "mock-handleTepSmsSending-send-tep-sms"
                      :puhelinnumero "+358401234567"
                      :body (elisa/msg-body "kysely.linkki/1"
                                            {:fi "Testilaitos"
                                             :en "Test Dept."
                                             :sv "Testanstalt"})}
                     {:type "mock-handleTepSmsSending-update-status-to-db"
                      :status "mock-lahetys"
                      :puhelinnumero "+358401234567"
                      :nippu {:ohjaaja_ytunnus_kj_tutkinto "test-id-1"
                              :niputuspvm "2021-12-15"
                              :voimassaloppupvm "2021-12-30"
                              :sms_kasittelytila
                              (:ei-lahetetty c/kasittelytilat)
                              :kyselylinkki "kysely.linkki/1"}
                      :new-loppupvm "2022-01-19"}
                     {:type "mock-handleTepSmsSending-patch-nippulinkki"
                      :kyselylinkki "kysely.linkki/1"
                      :data {:tila (:success c/kasittelytilat)
                             :voimassa_loppupvm "2022-01-19"}}
                     {:type "mock-handleTepSmsSending-query-items"
                      :ohjaaja_ytunnus_kj_tutkinto "test-id-2"
                      :niputuspvm "2021-12-15"}
                     {:type "mock-handleTepSmsSending-get-organisaatio"
                      :oppilaitos "1234"}
                     {:type "mock-handleTepSmsSending-send-tep-sms"
                      :puhelinnumero "+358401234567"
                      :body (elisa/msg-body "kysely.linkki/2"
                                            {:fi "Testilaitos"
                                             :en "Test Dept."
                                             :sv "Testanstalt"})}
                     {:type "mock-handleTepSmsSending-update-status-to-db"
                      :status "mock-lahetys"
                      :puhelinnumero "+358401234567"
                      :nippu {:ohjaaja_ytunnus_kj_tutkinto "test-id-2"
                              :niputuspvm "2021-12-15"
                              :voimassaloppupvm "2021-12-30"
                              :sms_kasittelytila
                              (:ei-lahetetty c/kasittelytilat)
                              :kyselylinkki "kysely.linkki/2"}
                      :new-loppupvm "2022-01-19"}
                     {:type "mock-handleTepSmsSending-patch-nippulinkki"
                      :kyselylinkki "kysely.linkki/2"
                      :data {:tila (:success c/kasittelytilat)
                             :voimassa_loppupvm "2022-01-19"}}]]
        (sh/-handleTepSmsSending {} event context)
        (= results (vec (reverse @test-handleTepSmsSending-results)))))))
