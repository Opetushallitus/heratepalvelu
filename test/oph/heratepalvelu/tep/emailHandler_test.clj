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
    (with-redefs [environ.core/env {:nippu-table "nippu-table-name"}
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2021 10 10))
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
    (with-redefs [environ.core/env {:nippu-table "nippu-table-name"}
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2021 10 10))
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

(def test-handleSendTEPEmails-call-log (atom []))

(defn- test-handleSendTEPEmails-add-to-call-log [item]
  (reset! test-handleSendTEPEmails-call-log
          (cons item @test-handleSendTEPEmails-call-log)))

(defn- mock-do-nippu-query []
  (test-handleSendTEPEmails-add-to-call-log "do-nippu-query")
  [{:id "email-1"
    :voimassaloppupvm "2021-10-11"}
   {:id "email-2"
    :voimassaloppupvm "2021-10-09"}
   {:id "email-3"
    :voimassaloppupvm "2021-10-09"}])

(defn- mock-get-jaksot-for-nippu [nippu]
  (test-handleSendTEPEmails-add-to-call-log
    (str "get-jaksot-for-nippu: " (:id nippu)))
  [{:oppilaitos "123.456.789"}])

(defn- mock-get-oppilaitokset [jaksot]
  (test-handleSendTEPEmails-add-to-call-log (str "get-oppilaitokset: " jaksot))
  (seq [{:en "Test Institution"
         :fi "Testilaitos"
         :sv "Testanstalt"}]))

(defn- mock-lahetysosoite [email jaksot]
  (test-handleSendTEPEmails-add-to-call-log
    (str "lahetysosoite: " (:id email) " " jaksot))
  (when (= "email-2" (:id email))
    "a@b.com"))

(defn- mock-send-survey-email [email oppilaitokset osoite]
  (test-handleSendTEPEmails-add-to-call-log
    (str "send-survey-email: " (:id email) " " oppilaitokset " " osoite))
  {:id 123})

(defn- mock-email-sent-update-item [email id lahetyspvm osoite]
  (test-handleSendTEPEmails-add-to-call-log
    (str "email-sent-update-item: "
         (:id email)
         " "
         id
         " "
         lahetyspvm
         " "
         osoite)))

(defn- mock-no-time-to-answer-update-item [email]
  (test-handleSendTEPEmails-add-to-call-log
    (str "no-time-to-answer-update-item: " (:id email))))

(defn- mock-has-time-to-answer? [pvm] (<= (compare pvm "2021-10-10") 0))

(deftest test-handleSendTEPEmails
  (testing "Varmista, että -handleSendTEPEmails kutsuu funktioita oikein"
    (with-redefs
      [clojure.tools.logging/log* tu/mock-log*
       oph.heratepalvelu.common/has-time-to-answer? mock-has-time-to-answer?
       oph.heratepalvelu.common/local-date-now (fn [] (LocalDate/of 2021 10 1))
       oph.heratepalvelu.tep.emailHandler/do-nippu-query mock-do-nippu-query
       oph.heratepalvelu.tep.emailHandler/lahetysosoite mock-lahetysosoite
       oph.heratepalvelu.tep.emailHandler/send-survey-email
       mock-send-survey-email
       oph.heratepalvelu.tep.emailHandler/email-sent-update-item
       mock-email-sent-update-item
       oph.heratepalvelu.tep.emailHandler/no-time-to-answer-update-item
       mock-no-time-to-answer-update-item
       oph.heratepalvelu.tep.tepCommon/get-jaksot-for-nippu
       mock-get-jaksot-for-nippu
       oph.heratepalvelu.tep.tepCommon/get-oppilaitokset
       mock-get-oppilaitokset]
      (let [event (tu/mock-handler-event :scheduledherate)
            context (tu/mock-handler-context)
            expected-call-log ["do-nippu-query"
                               "get-jaksot-for-nippu: email-1"
                               (str "get-oppilaitokset: "
                                    "[{:oppilaitos \"123.456.789\"}]")
                               (str "lahetysosoite: email-1 "
                                    "[{:oppilaitos \"123.456.789\"}]")
                               "no-time-to-answer-update-item: email-1"
                               "get-jaksot-for-nippu: email-2"
                               (str "get-oppilaitokset: "
                                    "[{:oppilaitos \"123.456.789\"}]")
                               (str "lahetysosoite: email-2 "
                                    "[{:oppilaitos \"123.456.789\"}]")
                               (str "send-survey-email: email-2 "
                                    "({:en \"Test Institution\", "
                                    ":fi \"Testilaitos\", :sv \"Testanstalt\"})"
                                    " a@b.com")
                               (str "email-sent-update-item: email-2 123 "
                                    "2021-10-01 a@b.com")
                               "get-jaksot-for-nippu: email-3"
                               (str "get-oppilaitokset: "
                                    "[{:oppilaitos \"123.456.789\"}]")
                               (str "lahetysosoite: email-3 "
                                    "[{:oppilaitos \"123.456.789\"}]")]]
        (eh/-handleSendTEPEmails {} event context)
        (is (= @test-handleSendTEPEmails-call-log (reverse expected-call-log)))
        (is (true? (tu/logs-contain?
                     {:level :info
                      :message "Käsitellään 3 lähetettävää viestiä."})))))))
