(ns oph.heratepalvelu.tep.EmailMuistutusHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]
            [oph.heratepalvelu.tep.EmailMuistutusHandler :as emh])
  (:import (java.time LocalDate)))

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
      (let [nippu {:ohjaaja_ytunnus_kj_tutkinto "test-nippu-id"
                   :niputuspvm "2021-10-10"}]
        (is (= (emh/do-jakso-query nippu) nippu))))))

(def test-get-oppilaitokset-result (atom []))

(defn- mock-get-organisaatio [oppilaitos]
  (reset! test-get-oppilaitokset-result
          (cons oppilaitos @test-get-oppilaitokset-result))
  {:nimi {:en "Test Institution"
          :fi "Testilaitos"
          :sv "Testanstalt"}})

(deftest test-get-oppilaitokset
  (testing "Varmista, että get-oppilaitokset kutsuu get-organisaatio oikein"
    (with-redefs [oph.heratepalvelu.external.organisaatio/get-organisaatio
                  mock-get-organisaatio]
      (let [jaksot [{:oppilaitos "123.456.789"}]
            expected (seq [{:en "Test Institution"
                            :fi "Testilaitos"
                            :sv "Testanstalt"}])
            expected-call-log (seq ["123.456.789"])]
        (is (= (emh/get-oppilaitokset jaksot) expected))
        (is (= @test-get-oppilaitokset-result expected-call-log))))))

(def test-send-reminder-email-result (atom {}))

(defn- mock-send-email [data] (reset! test-send-reminder-email-result data))

(deftest test-send-reminder-email
  (testing "Varmista, että send-reminder-email kutsuu send-email oikein"
    (with-redefs [oph.heratepalvelu.external.viestintapalvelu/send-email
                  mock-send-email]
      (let [nippu {:kyselylinkki "kysely.linkki/123"
                   :lahetysosoite "a@b.com"}
            oppilaitokset [{:en "Test Institution"
                            :fi "Testilaitos"
                            :sv "Testanstalt"}]
            expected {:subject (str "Muistutus-påminnelse-reminder: "
                                    "Työpaikkaohjaajakysely - "
                                    "Enkät till arbetsplatshandledaren - "
                                    "Survey to workplace instructors")
                    :body (vp/tyopaikkaohjaaja-muistutus-html nippu
                                                              oppilaitokset)
                    :address (:lahetysosoite nippu)
                    :sender "OPH – UBS – EDUFI"}]
        (emh/send-reminder-email nippu oppilaitokset)
        (is (= @test-send-reminder-email-result expected))))))

(def test-update-item-email-sent-result (atom {}))

(defn- mock-uies-update-item [query-params options table]
  (when (and (= :s (first (:ohjaaja_ytunnus_kj_tutkinto query-params)))
             (= :s (first (:niputuspvm query-params)))
             (= (str "SET #kasittelytila = :kasittelytila, #vpid = :vpid, "
                     "#muistutuspvm = :muistutuspvm, "
                     "#muistutukset = :muistutukset")
                (:update-expr options))
             (= "kasittelytila" (get (:expr-attr-names options)
                                     "#kasittelytila"))
             (= "viestintapalvelu-id" (get (:expr-attr-names options) "#vpid"))
             (= "email_muistutuspvm" (get (:expr-attr-names options)
                                          "#muistutuspvm"))
             (= "muistutukset" (get (:expr-attr-names options) "#muistutukset"))
             (= :s (first (get (:expr-attr-vals options) ":kasittelytila")))
             (= :n (first (get (:expr-attr-vals options) ":vpid")))
             (= :s (first (get (:expr-attr-vals options) ":muistutuspvm")))
             (= :n (first (get (:expr-attr-vals options) ":muistutukset")))
             (= 1 (second (get (:expr-attr-vals options) ":muistutukset")))
             (= "nippu-table-name" table))
    (reset! test-update-item-email-sent-result
            {:ohjaaja_ytunnus_kj_tutkinto (second (:ohjaaja_ytunnus_kj_tutkinto
                                                    query-params))
             :niputuspvm (second (:niputuspvm query-params))
             :kasittelytila (second (get (:expr-attr-vals options)
                                         ":kasittelytila"))
             :vpid (second (get (:expr-attr-vals options) ":vpid"))
             :muistutuspvm (second (get (:expr-attr-vals options)
                                        ":muistutuspvm"))})))

(deftest test-update-item-email-sent
  (testing "Varmista, että update-item-email-sent kutsuu update-item oikein"
    (with-redefs
      [environ.core/env {:nippu-table "nippu-table-name"}
       oph.heratepalvelu.db.dynamodb/update-item mock-uies-update-item]
      (let [nippu {:ohjaaja_ytunnus_kj_tutkinto "test-nippu-id"
                   :niputuspvm "2021-10-05"}
            id 123
            expected {:ohjaaja_ytunnus_kj_tutkinto "test-nippu-id"
                      :niputuspvm "2021-10-05"
                      :kasittelytila (:viestintapalvelussa c/kasittelytilat)
                      :vpid 123
                      :muistutuspvm (str (LocalDate/now))}]
        (emh/update-item-email-sent nippu id)
        (is (= @test-update-item-email-sent-result expected))))))

(def test-update-item-cannot-answer-result (atom {}))

(defn- mock-uica-update-item [query-params options table]
  (when (and (= :s (first (:ohjaaja_ytunnus_kj_tutkinto query-params)))
             (= :s (first (:niputuspvm query-params)))
             (= (str "SET #kasittelytila = :kasittelytila, "
                     "#muistutukset = :muistutukset")
                (:update-expr options))
             (= "kasittelytila" (get (:expr-attr-names options)
                                     "#kasittelytila"))
             (= "muistutukset" (get (:expr-attr-names options) "#muistutukset"))
             (= :s (first (get (:expr-attr-vals options) ":kasittelytila")))
             (= :n (first (get (:expr-attr-vals options) ":muistutukset")))
             (= 1 (second (get (:expr-attr-vals options) ":muistutukset")))
             (= "nippu-table-name" table))
    (reset! test-update-item-cannot-answer-result
            {:ohjaaja_ytunnus_kj_tutkinto (second (:ohjaaja_ytunnus_kj_tutkinto
                                                    query-params))
             :niputuspvm (second (:niputuspvm query-params))
             :kasittelytila (second (get (:expr-attr-vals options)
                                         ":kasittelytila"))})))

(deftest test-update-item-cannot-answer
  (testing "Varmista, että update-item-cannot-answer kutsuu update-item oikein"
    (with-redefs
      [environ.core/env {:nippu-table "nippu-table-name"}
       oph.heratepalvelu.db.dynamodb/update-item mock-uica-update-item]
      (let [nippu {:ohjaaja_ytunnus_kj_tutkinto "test-nippu-id"
                   :niputuspvm "2021-10-10"}
            status1 {:vastattu true}
            status2 {:vastattu false}
            expected1 {:ohjaaja_ytunnus_kj_tutkinto "test-nippu-id"
                       :niputuspvm "2021-10-10"
                       :kasittelytila (:vastattu c/kasittelytilat)}
            expected2 {:ohjaaja_ytunnus_kj_tutkinto "test-nippu-id"
                       :niputuspvm "2021-10-10"
                       :kasittelytila (:vastausaika-loppunut-m
                                        c/kasittelytilat)}]
        (emh/update-item-cannot-answer nippu status1)
        (is (= @test-update-item-cannot-answer-result expected1))
        (reset! test-update-item-cannot-answer-result {})
        (emh/update-item-cannot-answer nippu status2)
        (is (= @test-update-item-cannot-answer-result expected2))))))
