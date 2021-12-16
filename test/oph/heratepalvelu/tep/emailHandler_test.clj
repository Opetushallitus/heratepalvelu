(ns oph.heratepalvelu.tep.emailHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.tep.emailHandler :as eh]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(use-fixtures :each tu/clear-logs-before-test)

(deftest test-bad-phone?
  (testing "Varmista, että bad-phone? palauttaa oikeita arvoja"
    (let [nippu1 {:sms_kasittelytila (:phone-mismatch c/kasittelytilat)}
          nippu2 {:sms_kasittelytila (:no-phone c/kasittelytilat)}
          nippu3 {:sms_kasittelytila (:phone-invalid c/kasittelytilat)}
          nippu4 {:sms_kasittelytila (:success c/kasittelytilat)}]
      (is (true? (eh/bad-phone? nippu1)))
      (is (true? (eh/bad-phone? nippu2)))
      (is (true? (eh/bad-phone? nippu3)))
      (is (false? (eh/bad-phone? nippu4))))))

(def test-lahetysosoite-update-item-results (atom {}))

(defn- mock-lahetysosoite-update-item-update-item [query-params options table]
  (when (and (= :s (first (:ohjaaja_ytunnus_kj_tutkinto query-params)))
             (= :s (first (:niputuspvm query-params)))
             (= "SET #kasittelytila = :kasittelytila" (:update-expr options))
             (= "kasittelytila" (get (:expr-attr-names options)
                                     "#kasittelytila"))
             (= :s (first (get (:expr-attr-vals options) ":kasittelytila")))
             (= "nippu-table-name" table))
    (reset! test-lahetysosoite-update-item-results
            {:ohjaaja_ytunnus_kj_tutkinto (second (:ohjaaja_ytunnus_kj_tutkinto
                                                    query-params))
             :niputuspvm (second (:niputuspvm query-params))
             :kasittelytila (second (get (:expr-attr-vals options)
                                         ":kasittelytila"))})))

(deftest test-lahetysosoite-update-item
  (testing "Varmista, että lahetysosoite-update-item kutsuu update-item oikein"
    (with-redefs [environ.core/env {:nippu-table "nippu-table-name"}
                  oph.heratepalvelu.db.dynamodb/update-item
                  mock-lahetysosoite-update-item-update-item]
      (let [nippu {:ohjaaja_ytunnus_kj_tutkinto "test-nippu-id"
                   :niputuspvm "2021-10-10"}
            osoitteet ["a@b.com"]
            osoitteet-empty []
            expected {:ohjaaja_ytunnus_kj_tutkinto "test-nippu-id"
                      :niputuspvm "2021-10-10"
                      :kasittelytila (:email-mismatch c/kasittelytilat)}
            expected-empty {:ohjaaja_ytunnus_kj_tutkinto "test-nippu-id"
                            :niputuspvm "2021-10-10"
                            :kasittelytila (:no-email c/kasittelytilat)}]
        (eh/lahetysosoite-update-item nippu osoitteet)
        (is (= @test-lahetysosoite-update-item-results expected))
        (eh/lahetysosoite-update-item nippu osoitteet-empty)
        (is (= @test-lahetysosoite-update-item-results expected-empty))))))

(deftest test-get-single-ohjaaja-email
  (testing (str "Varmista, että get-single-ohjaaja-email palauttaa yhteisen"
                " sähköpostin, jos kaikissa jaksoissa on sama sähköposti;"
                " muuten palauttaa nil.")
    (let [jaksot-same-emails [{:ohjaaja_email "a@b.com"}
                              {:ohjaaja_email "a@b.com"}]
          jaksot-different-emails [{:ohjaaja_email "a@b.com"}
                                   {:ohjaaja_email "x@y.com"}]]
      (is (= "a@b.com" (eh/get-single-ohjaaja-email jaksot-same-emails)))
      (is (nil? (eh/get-single-ohjaaja-email jaksot-different-emails))))))

(def mock-lahetysosoite-update-item-result (atom {}))

(defn- mock-lahetysosoite-update-item [nippu osoitteet]
  (reset! mock-lahetysosoite-update-item-result {:nippu nippu
                                                 :osoitteet osoitteet}))

(def mock-patch-nippulinkki-result (atom {}))

(defn- mock-patch-nippulinkki [linkki data]
  (reset! mock-patch-nippulinkki-result {:kyselylinkki linkki :data data}))

(defn- reset-lahetysosoite-result-variables []
  (reset! mock-lahetysosoite-update-item-result {})
  (reset! mock-patch-nippulinkki-result {}))

(deftest test-lahetysosoite
  (testing "Varmista, että lahetysosoite kutsuu oikeita funktioita"
    (with-redefs [clojure.tools.logging/log* tu/mock-log*
                  oph.heratepalvelu.external.arvo/patch-nippulinkki
                  mock-patch-nippulinkki
                  oph.heratepalvelu.tep.emailHandler/lahetysosoite-update-item
                  mock-lahetysosoite-update-item]
      (let [nippu {:ohjaaja_ytunnus_kj_tutkinto "test-nippu-id"
                   :niputuspvm "2021-10-10"
                   :sms_kasittelytila (:success c/kasittelytilat)
                   :kyselylinkki "kysely.linkki/123"}
            nippu-bad-phone {:ohjaaja_ytunnus_kj_tutkinto "test-bad-phone-id"
                             :niputuspvm "2021-10-10"
                             :sms_kasittelytila (:phone-mismatch
                                                  c/kasittelytilat)
                             :kyselylinkki "kysely.linkki/1234"}
            jaksot-same-emails [{:ohjaaja_email "a@b.com"}
                                {:ohjaaja_email "a@b.com"}]
            jaksot-different-emails [{:ohjaaja_email "a@b.com"}
                                     {:ohjaaja_email "x@y.com"}]]
        (is (= (eh/lahetysosoite nippu jaksot-same-emails) "a@b.com"))
        (reset-lahetysosoite-result-variables)
        (is (nil? (eh/lahetysosoite nippu jaksot-different-emails)))
        (is (= @mock-lahetysosoite-update-item-result
               {:nippu nippu
                :osoitteet #{"a@b.com" "x@y.com"}}))
        (is (= @mock-patch-nippulinkki-result {}))
        (is (true? (tu/logs-contain?
                     {:level :warn
                      :message (str "Ei yksiselitteistä ohjaajan sähköpostia  "
                                    "test-nippu-id , 2021-10-10 , "
                                    "#{a@b.com x@y.com}")})))
        (reset-lahetysosoite-result-variables)
        (is (nil? (eh/lahetysosoite nippu-bad-phone jaksot-different-emails)))
        (is (= @mock-lahetysosoite-update-item-result
               {:nippu nippu-bad-phone
                :osoitteet #{"a@b.com" "x@y.com"}}))
        (is (= @mock-patch-nippulinkki-result
               {:kyselylinkki "kysely.linkki/1234"
                :data {:tila (:ei-yhteystietoja c/kasittelytilat)}}))
        (is (true? (tu/logs-contain?
                     {:level :warn
                      :message (str "Ei yksiselitteistä ohjaajan sähköpostia  "
                                    "test-bad-phone-id , 2021-10-10 , "
                                    "#{a@b.com x@y.com}")})))))))

(def test-do-nippu-query-results (atom ""))

(defn- mock-do-nippu-query-query-items [query-params options table]
  (when (and (= :eq (first (:kasittelytila query-params)))
             (= :s (first (second (:kasittelytila query-params))))
             (= (:ei-lahetetty c/kasittelytilat)
                (second (second (:kasittelytila query-params))))
             (= :le (first (:niputuspvm query-params)))
             (= :s (first (second (:niputuspvm query-params))))
             (= "niputusIndex" (:index options))
             (= 20 (:limit options))
             (= "nippu-table-name" table))
    (reset! test-do-nippu-query-results
            (second (second (:niputuspvm query-params))))))

(deftest test-do-nippu-query
  (testing "Varmista, että do-nippu-query kutsuu query-items oikein"
    (with-redefs [clj-time.core/today (fn [] (LocalDate/of 2021 10 10))
                  environ.core/env {:nippu-table "nippu-table-name"}
                  oph.heratepalvelu.db.dynamodb/query-items
                  mock-do-nippu-query-query-items]
      (eh/do-nippu-query)
      (is (= @test-do-nippu-query-results "2021-10-10")))))

(def test-email-sent-update-item-results (atom {}))

(defn- mock-esui-update-item [query-params options table]
  (when (and (= :s (first (:ohjaaja_ytunnus_kj_tutkinto query-params)))
             (= :s (first (:niputuspvm query-params)))
             (= (str "SET #kasittelytila = :kasittelytila, #vpid = :vpid, "
                     "#lahetyspvm = :lahetyspvm, #muistutukset = :muistutukset,"
                     " #lahetysosoite = :lahetysosoite")
                (:update-expr options))
             (= "kasittelytila" (get (:expr-attr-names options)
                                     "#kasittelytila"))
             (= "viestintapalvelu-id" (get (:expr-attr-names options) "#vpid"))
             (= "lahetyspvm" (get (:expr-attr-names options) "#lahetyspvm"))
             (= "muistutukset" (get (:expr-attr-names options) "#muistutukset"))
             (= "lahetysosoite" (get (:expr-attr-names options)
                                     "#lahetysosoite"))
             (= :s (first (get (:expr-attr-vals options) ":kasittelytila")))
             (= :n (first (get (:expr-attr-vals options) ":vpid")))
             (= :s (first (get (:expr-attr-vals options) ":lahetyspvm")))
             (= :n (first (get (:expr-attr-vals options) ":muistutukset")))
             (= :s (first (get (:expr-attr-vals options) ":lahetysosoite")))
             (= "nippu-table-name" table))
    (reset! test-email-sent-update-item-results
            {:ohjaaja_ytunnus_kj_tutkinto (second (:ohjaaja_ytunnus_kj_tutkinto
                                                    query-params))
             :niputuspvm (second (:niputuspvm query-params))
             :kasittelytila (second (get (:expr-attr-vals options)
                                         ":kasittelytila"))
             :vpid (second (get (:expr-attr-vals options) ":vpid"))
             :lahetyspvm (second (get (:expr-attr-vals options) ":lahetyspvm"))
             :muistutukset (second (get (:expr-attr-vals options)
                                        ":muistutukset"))
             :lahetysosoite (second (get (:expr-attr-vals options)
                                         ":lahetysosoite"))})))

(deftest test-email-sent-update-item
  (testing "Varmista, että email-sent-update-item kutsuu update-item oikein"
    (with-redefs [environ.core/env {:nippu-table "nippu-table-name"}
                  oph.heratepalvelu.db.dynamodb/update-item
                  mock-esui-update-item]
      (let [email {:ohjaaja_ytunnus_kj_tutkinto "test-nippu-id"
                   :niputuspvm "2021-10-08"}
            id 123
            lahetyspvm "2021-10-10"
            osoite "a@b.com"
            expected {:ohjaaja_ytunnus_kj_tutkinto "test-nippu-id"
                      :niputuspvm "2021-10-08"
                      :kasittelytila (:viestintapalvelussa c/kasittelytilat)
                      :vpid 123
                      :lahetyspvm "2021-10-10"
                      :muistutukset 0
                      :lahetysosoite "a@b.com"}]
        (eh/email-sent-update-item email id lahetyspvm osoite)
        (is (= @test-email-sent-update-item-results expected))))))

(def test-send-survey-email-results (atom {}))

(deftest test-send-survey-email
  (testing "Varmista, että send-survey-email kutsuu send-email oikein"
    (with-redefs [oph.heratepalvelu.external.viestintapalvelu/send-email
                  (fn [obj] (reset! test-send-survey-email-results obj))]
      (let [email {:kyselylinkki "kysely.linkki/123"}
            oppilaitokset [{:en "Test Institution"
                            :fi "Testilaitos"
                            :sv "Testanstalt"}]
            osoite "a@b.com"
            expected {:subject (str "Työpaikkaohjaajakysely - "
                                    "Enkät till arbetsplatshandledaren - "
                                    "Survey to workplace instructors")
                      :body (vp/tyopaikkaohjaaja-html email oppilaitokset)
                      :address "a@b.com"
                      :sender "OPH – UBS – EDUFI"}]
        (eh/send-survey-email email oppilaitokset osoite)
        (is (= @test-send-survey-email-results expected))))))

(def test-no-time-to-answer-update-item-results (atom {}))

(defn- mock-nttaui-update-item [query-params options table]
  (when (and (= :s (first (:ohjaaja_ytunnus_kj_tutkinto query-params)))
             (= :s (first (:niputuspvm query-params)))
             (= "SET #kasittelytila = :kasittelytila, #lahetyspvm = :lahetyspvm"
                (:update-expr options))
             (= "kasittelytila" (get (:expr-attr-names options)
                                     "#kasittelytila"))
             (= "lahetyspvm" (get (:expr-attr-names options) "#lahetyspvm"))
             (= :s (first (get (:expr-attr-vals options) ":kasittelytila")))
             (= :s (first (get (:expr-attr-vals options) ":lahetyspvm")))
             (= "nippu-table-name" table))
    (reset! test-no-time-to-answer-update-item-results
            {:ohjaaja_ytunnus_kj_tutkinto (second (:ohjaaja_ytunnus_kj_tutkinto
                                                    query-params))
             :niputuspvm (second (:niputuspvm query-params))
             :kasittelytila (second (get (:expr-attr-vals options)
                                         ":kasittelytila"))
             :lahetyspvm (second (get (:expr-attr-vals options)
                                      ":lahetyspvm"))})))

(deftest test-no-time-to-answer-update-item
  (testing "Varmista, että no-time-to-answer-update-item kutsuu update-item"
    (with-redefs [clj-time.core/today (fn [] (LocalDate/of 2021 10 10))
                  environ.core/env {:nippu-table "nippu-table-name"}
                  oph.heratepalvelu.db.dynamodb/update-item
                  mock-nttaui-update-item]
      (let [email {:ohjaaja_ytunnus_kj_tutkinto "test-nippu-id"
                   :niputuspvm "2021-09-09"}
            expected {:ohjaaja_ytunnus_kj_tutkinto "test-nippu-id"
                      :niputuspvm "2021-09-09"
                      :kasittelytila (:vastausaika-loppunut c/kasittelytilat)
                      :lahetyspvm "2021-10-10"}]
        (eh/no-time-to-answer-update-item email)
        (is (= @test-no-time-to-answer-update-item-results expected))))))

(defn- mock-jakso-query-query-items [query-params options table]
  (when (and (= :eq (first (:ohjaaja_ytunnus_kj_tutkinto query-params)))
             (= :s (first (second (:ohjaaja_ytunnus_kj_tutkinto query-params))))
             (= :eq (first (:niputuspvm query-params)))
             (= :s (first (second (:niputuspvm query-params))))
             (= "niputusIndex" (:index options))
             (= "jaksotunnus-table-name" table))
    {:ohjaaja_ytunnus_kj_tutkinto (second (second (:ohjaaja_ytunnus_kj_tutkinto
                                                    query-params)))
     :niputuspvm (second (second (:niputuspvm query-params)))}))

(deftest test-do-jakso-query
  (testing "Varmista, että do-jakso-query kutsuu query-items oikein"
    (with-redefs [environ.core/env {:jaksotunnus-table "jaksotunnus-table-name"}
                  oph.heratepalvelu.db.dynamodb/query-items
                  mock-jakso-query-query-items]
      (let [email {:ohjaaja_ytunnus_kj_tutkinto "test-email-id"
                   :niputuspvm "2021-10-10"}]
        (is (= (eh/do-jakso-query email) email))))))

(def test-get-oppilaitokset-results (atom ""))

(defn- mock-get-organisaatio [oppilaitoksen-oid]
  (reset! test-get-oppilaitokset-results
          (str @test-get-oppilaitokset-results oppilaitoksen-oid " "))
  {:nimi {:en "Test Institution"
          :fi "Testilaitos"
          :sv "Testanstalt"}})

(deftest test-get-oppilaitokset
  (testing "Varmista, että get-oppilaitokset toimii oikein"
    (with-redefs [oph.heratepalvelu.external.organisaatio/get-organisaatio
                  mock-get-organisaatio]
      (let [jaksot [{:oppilaitos "123.456.789"}]
            expected (seq #{{:en "Test Institution"
                             :fi "Testilaitos"
                             :sv "Testanstalt"}})
            expected-call-log-results "123.456.789 "]
        (is (= (eh/get-oppilaitokset jaksot) expected))
        (is (= @test-get-oppilaitokset-results expected-call-log-results))))))





;; TODO -handleSendTEPEmails
