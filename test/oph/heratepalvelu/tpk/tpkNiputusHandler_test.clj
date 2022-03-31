(ns oph.heratepalvelu.tpk.tpkNiputusHandler-test
  (:require [clj-time.core :as t]
            [clojure.test :refer :all]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.tpk.tpkNiputusHandler :as tpk]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(deftest test-check-jakso
  (testing "check-jakso? tarkistaa jaksot oikein"
    (let [good-jakso1 {:koulutustoimija        "1.2.246.562.10.346830761110"
                       :tyopaikan_nimi         "Testi työpaikka"
                       :tyopaikan_ytunnus      "1234567-8"
                       :jakso_loppupvm         "2021-11-20"
                       :hankkimistapa_tyyppi   "koulutussopimus"}
          good-jakso2 {:koulutustoimija        "1.2.246.562.10.346830761110"
                       :tyopaikan_nimi         "Testi työpaikka"
                       :tyopaikan_ytunnus      "1234567-8"
                       :jakso_loppupvm         "2021-11-20"
                       :hankkimistapa_tyyppi   "oppisopimus"
                       :oppisopimuksen_perusta "01"}
          good-jakso3 {:koulutustoimija        "1.2.246.562.10.346830761110"
                       :tyopaikan_nimi         "Testi työpaikka"
                       :tyopaikan_ytunnus      "1234567-8"
                       :jakso_loppupvm         "2021-11-20"
                       :hankkimistapa_tyyppi   "oppisopimus"}
          bad-jakso1 {:koulutustoimija        "1.2.246.562.10.346830761110"
                      :tyopaikan_nimi         "Testi työpaikka"
                      :tyopaikan_ytunnus      "1234567-8"
                      :jakso_loppupvm         "2021-11-20"
                      :hankkimistapa_tyyppi   "oppisopimus"
                      :oppisopimuksen_perusta "02"}
          bad-jakso2 {:koulutustoimija        "1.2.246.562.10.346830761110"
                      :tyopaikan_ytunnus      "1234567-8"
                      :jakso_loppupvm         "2021-11-20"
                      :hankkimistapa_tyyppi   "oppisopimus"
                      :oppisopimuksen_perusta "01"}
          bad-jakso3 {:tyopaikan_nimi         "Testi työpaikka"
                      :tyopaikan_ytunnus      "1234567-8"
                      :jakso_loppupvm         "2021-11-20"
                      :hankkimistapa_tyyppi   "oppisopimus"
                      :oppisopimuksen_perusta "01"}
          bad-jakso4 {:koulutustoimija        "1.2.246.562.10.346830761110"
                      :tyopaikan_nimi         "Testi työpaikka"
                      :jakso_loppupvm         "2021-11-20"
                      :hankkimistapa_tyyppi   "oppisopimus"
                      :oppisopimuksen_perusta "01"}
          bad-jakso5 {:koulutustoimija        "1.2.246.562.10.346830761110"
                      :tyopaikan_nimi         "Testi työpaikka"
                      :tyopaikan_ytunnus      "1234567-8"
                      :hankkimistapa_tyyppi   "oppisopimus"
                      :oppisopimuksen_perusta "01"}]
      (is (tpk/check-jakso? good-jakso1))
      (is (tpk/check-jakso? good-jakso2))
      (is (tpk/check-jakso? good-jakso3))
      (is (not (tpk/check-jakso? bad-jakso1)))
      (is (not (tpk/check-jakso? bad-jakso2)))
      (is (not (tpk/check-jakso? bad-jakso3)))
      (is (not (tpk/check-jakso? bad-jakso4)))
      (is (not (tpk/check-jakso? bad-jakso5))))))

(deftest test-create-nippu-id
  (testing "create-nippu-id luo nipun ID:n oikein"
    (let [jakso1 {:koulutustoimija        "1.2.246.562.10.346830761110"
                  :tyopaikan_nimi         "Testi työpaikka"
                  :tyopaikan_ytunnus      "1234567-8"
                  :jakso_loppupvm         "2021-11-20"
                  :hankkimistapa_tyyppi   "koulutussopimus"}]
      (is (= "testi_tyopaikka/1234567-8/1.2.246.562.10.346830761110/2021-07-01_2021-12-31"
             (tpk/create-nippu-id jakso1))))))

(deftest test-get-next-vastaamisajan-alkupvm-date
  (testing "get-next-vastaamisajan-alkupvm-date palauttaa oikean arvon"
    (let [jakso1 {:koulutustoimija        "1.2.246.562.10.346830761110"
                  :tyopaikan_nimi         "Testi työpaikka"
                  :tyopaikan_ytunnus      "1234567-8"
                  :jakso_loppupvm         "2021-11-20"
                  :hankkimistapa_tyyppi   "koulutussopimus"}
          jakso2 {:koulutustoimija        "1.2.246.562.10.346830761110"
                  :tyopaikan_nimi         "Testi työpaikka"
                  :tyopaikan_ytunnus      "1234567-8"
                  :jakso_loppupvm         "2022-02-02"
                  :hankkimistapa_tyyppi   "koulutussopimus"}]
      (is (= "2022-01-01"
             (str (tpk/get-next-vastaamisajan-alkupvm-date jakso1))))
      (is (= "2022-07-01"
             (str (tpk/get-next-vastaamisajan-alkupvm-date jakso2)))))))

(deftest test-create-tpk-nippu
  (testing "Varmistaa, että create-nippu luo niput oikein"
    (let [jakso {:koulutustoimija        "1.2.246.562.10.346830761110"
                 :tyopaikan_nimi         "Testi työpaikka"
                 :tyopaikan_ytunnus      "1234567-8"
                 :jakso_loppupvm         "2021-11-20"
                 :hankkimistapa_tyyppi   "koulutussopimus"}]
      (is (= (tpk/create-tpk-nippu jakso)
             {:nippu-id
              "testi_tyopaikka/1234567-8/1.2.246.562.10.346830761110/2021-07-01_2021-12-31"
              :tyopaikan-nimi               "Testi työpaikka"
              :tyopaikan-nimi-normalisoitu  "testi_tyopaikka"
              :vastaamisajan-alkupvm        "2022-01-01"
              :vastaamisajan-loppupvm       "2022-02-28"
              :tyopaikan-ytunnus            "1234567-8"
              :koulutustoimija-oid          "1.2.246.562.10.346830761110"
              :tiedonkeruu-alkupvm          "2021-07-01"
              :tiedonkeruu-loppupvm         "2021-12-31"
              :niputuspvm                   (str (t/today))})))))

(deftest test-get-existing-nippu
  (testing "Varmistaa, että get-existing-nippu kutsuu get-item oikein"
    (with-redefs [environ.core/env {:tpk-nippu-table "tpk-nippu-table-name"}
                  oph.heratepalvelu.db.dynamodb/get-item
                  (fn [query-params table]
                    (when (and (= :s (first (:nippu-id query-params)))
                               (= table "tpk-nippu-table-name"))
                      {:nippu-id (second (:nippu-id query-params))}))]
      (let [jakso {:tyopaikan_nimi "Ääkköset Által"
                   :tyopaikan_ytunnus "123456-7"
                   :koulutustoimija "test-kt-id"
                   :jakso_loppupvm "2021-10-10"}
            expected {:nippu-id (str "aakkoset_altal/123456-7/test-kt-id/"
                                     "2021-07-01_2021-12-31")}]
        (is (= (tpk/get-existing-nippu jakso) expected))))))

(def test-save-nippu-results (atom {}))

(defn- mock-put-item [query-params options table]
  (when (and (= :s (first (:nippu-id query-params)))
             (= :s (first (:tyopaikan-nimi query-params)))
             (= :s (first (:tyopaikan-nimi-normalisoitu query-params)))
             (= :s (first (:vastaamisajan-alkupvm query-params)))
             (= :s (first (:vastaamisajan-loppupvm query-params)))
             (= :s (first (:tyopaikan-ytunnus query-params)))
             (= :s (first (:koulutustoimija-oid query-params)))
             (= :s (first (:tiedonkeruu-alkupvm query-params)))
             (= :s (first (:tiedonkeruu-loppupvm query-params)))
             (= :s (first (:niputuspvm query-params)))
             (= {} options)
             (= "tpk-nippu-table-name" table))
    (reset! test-save-nippu-results
            {:nippu-id (second (:nippu-id query-params))
             :tyopaikan-nimi (second (:tyopaikan-nimi query-params))
             :tyopaikan-nimi-normalisoitu
             (second (:tyopaikan-nimi-normalisoitu query-params))
             :vastaamisajan-alkupvm (second
                                      (:vastaamisajan-alkupvm query-params))
             :vastaamisajan-loppupvm (second
                                       (:vastaamisajan-loppupvm query-params))
             :tyopaikan-ytunnus (second (:tyopaikan-ytunnus query-params))
             :koulutustoimija-oid (second (:koulutustoimija-oid query-params))
             :tiedonkeruu-alkupvm (second (:tiedonkeruu-alkupvm query-params))
             :tiedonkeruu-loppupvm (second (:tiedonkeruu-loppupvm query-params))
             :niputuspvm (second (:niputuspvm query-params))})))

(deftest test-save-nippu
  (testing "Varmistaa, että save-nippu kutsuu put-item oikein"
    (with-redefs [environ.core/env {:tpk-nippu-table "tpk-nippu-table-name"}
                  oph.heratepalvelu.db.dynamodb/put-item mock-put-item]
      (let [nippu {:nippu-id "aakkoset/123456-7/test-id/2021-07-01_2021-12-31"
                   :tyopaikan-nimi "Ääkköset"
                   :tyopaikan-nimi-normalisoitu "aakkoset"
                   :vastaamisajan-alkupvm "2022-01-15"
                   :vastaamisajan-loppupvm "2022-02-28"
                   :tyopaikan-ytunnus "123456-7"
                   :koulutustoimija-oid "test-id"
                   :tiedonkeruu-alkupvm "2021-07-01"
                   :tiedonkeruu-loppupvm "2021-12-31"
                   :niputuspvm "2021-12-15"}]
        (tpk/save-tpk-nippu nippu)
        (is (= @test-save-nippu-results nippu))))))

(def test-update-tpk-niputuspvm-results (atom {}))

(defn- mock-update-item [query-params options table]
  (when (and (= :n (first (:hankkimistapa_id query-params)))
             (= "SET #value = :value" (:update-expr options))
             (= "tpk-niputuspvm" (get (:expr-attr-names options) "#value"))
             (= :s (first (get (:expr-attr-vals options) ":value")))
             (= "jaksotunnus-table-name" table))
    (reset! test-update-tpk-niputuspvm-results
            {:hankkimistapa_id (second (:hankkimistapa_id query-params))
             :new-value (second (get (:expr-attr-vals options) ":value"))})))

(deftest test-update-tpk-niputuspvm
  (testing "Varmistaa, että update-tpk-niputuspvm kutsuu update-item oikein"
    (with-redefs [environ.core/env {:jaksotunnus-table "jaksotunnus-table-name"}
                  oph.heratepalvelu.db.dynamodb/update-item mock-update-item]
      (let [jakso {:hankkimistapa_id 123
                   :tyopaikan_nimi "Ääkköset Által"
                   :tyopaikan_ytunnus "123456-7"
                   :koulutustoimija "test-kt-id"
                   :jakso_loppupvm "2021-10-10"}
            new-value "2021-12-15"
            expected {:hankkimistapa_id 123
                      :new-value "2021-12-15"}]
        (tpk/update-tpk-niputuspvm jakso new-value)
        (is (= @test-update-tpk-niputuspvm-results expected))))))

(defn- results-with-end-date [end-date]
  {:filter-expression "#tpkNpvm = :tpkNpvm AND #jl BETWEEN :start AND :end"
   :exclusive-start-key "asdf"
   :expr-attr-names {"#tpkNpvm" "tpk-niputuspvm"
                     "#jl"      "jakso_loppupvm"}
   :expr-attr-vals {":tpkNpvm" [:s "ei_maaritelty"]
                    ":end"     [:s end-date]
                    ":start"   [:s "2021-07-01"]}
   :table "jaksotunnus-table-name"})

(deftest test-query-niputtamattomat
  (testing "Varmistaa, etta query-niputtamattomat kutsuu scan oikein"
    (with-redefs [environ.core/env {:jaksotunnus-table "jaksotunnus-table-name"}
                  oph.heratepalvelu.db.dynamodb/scan
                  (fn [options table] (assoc options :table table))]
      (with-redefs [oph.heratepalvelu.common/local-date-now
                    (fn [] (LocalDate/of 2021 12 25))]
        (is (= (tpk/query-niputtamattomat "asdf")
               (results-with-end-date "2021-12-25"))))
      (with-redefs [oph.heratepalvelu.common/local-date-now
                    (fn [] (LocalDate/of 2022 2 2))]
        (is (= (tpk/query-niputtamattomat "asdf")
               (results-with-end-date "2021-12-31")))))))

(def mock-handleTpkNiputus-results (atom []))

(defn- append-to-mock-handleTpkNiputus-results [value]
  (reset! mock-handleTpkNiputus-results
          (cons value @mock-handleTpkNiputus-results)))

(def mock-niputtamattomat-list
  [{:hankkimistapa_id [:n 863]
    :koulutustoimija [:s "12345"]
    :tyopaikan_nimi [:s "Työ Paikka"]
    :tyopaikan_ytunnus [:s "123456-7"]
    :jakso_loppupvm [:s "2021-12-20"]
    :hankkimistapa_tyyppi [:s "oppisopimus"]
    :oppisopimuksen_perusta [:s "02"]}
   {:hankkimistapa_id [:n 1]
    :koulutustoimija [:s "12345"]
    :tyopaikan_nimi [:s "Työ Paikka"]
    :tyopaikan_ytunnus [:s "123456-7"]
    :jakso_loppupvm [:s "2021-12-20"]
    :hankkimistapa_tyyppi [:s "koulutussopimus"]
    :oppisopimuksen_perusta [:s "02"]}

   ;; Tämä jakso kuuluu memoized-nippuun
   {:hankkimistapa_id [:n 2]
    :koulutustoimija [:s "12345"]
    :tyopaikan_nimi [:s "Työ Paikka"]
    :tyopaikan_ytunnus [:s "123456-7"]
    :jakso_loppupvm [:s "2021-12-20"]
    :hankkimistapa_tyyppi [:s "koulutussopimus"]
    :oppisopimuksen_perusta [:s "02"]}

   ;; Tämä jakso kuuluu olemassa olevaan nippuun
   {:hankkimistapa_id [:n 1234]
    :koulutustoimija [:s "12345"]
    :tyopaikan_nimi [:s "Toinen Työ Paikka"]
    :tyopaikan_ytunnus [:s "123890-6"]
    :jakso_loppupvm [:s "2021-12-20"]
    :hankkimistapa_tyyppi [:s "koulutussopimus"]
    :oppisopimuksen_perusta [:s "02"]}])

(defn- get-specific-niputtamaton [index]
  (reduce #(assoc %1 (first %2) (second (second %2)))
          {}
          (seq (get mock-niputtamattomat-list index))))

(defn- mock-query-niputtamattomat [exclusive-start-key]
  {:items (map
            ddb/map-attribute-values-to-vals
            (map ddb/map-vals-to-attribute-values mock-niputtamattomat-list))})

(defn- mock-get-existing-nippu [jakso]
  (append-to-mock-handleTpkNiputus-results {:type "mock-get-existing-nippu"
                                            :jakso jakso})
  (when (= 1234 (:hankkimistapa_id jakso)) {:niputuspvm "2021-12-15"}))

(defn- mock-save-tpk-nippu [nippu]
  (append-to-mock-handleTpkNiputus-results {:type "mock-save-tpk-nippu"
                                            :nippu (dissoc nippu :request-id)}))

(defn- mock-update-tpk-niputuspvm [jakso new-value]
  (append-to-mock-handleTpkNiputus-results {:type "mock-update-tpk-niputuspvm"
                                            :jakso-id (:hankkimistapa_id jakso)
                                            :new-value new-value}))

(deftest test-handleTpkNiputus
  (testing "Varmista, että -handleTpkNiputus toimii oikein"
    (with-redefs
      [oph.heratepalvelu.tpk.tpkNiputusHandler/get-existing-nippu
       mock-get-existing-nippu
       oph.heratepalvelu.tpk.tpkNiputusHandler/query-niputtamattomat
       mock-query-niputtamattomat
       oph.heratepalvelu.tpk.tpkNiputusHandler/save-tpk-nippu
       mock-save-tpk-nippu
       oph.heratepalvelu.tpk.tpkNiputusHandler/update-tpk-niputuspvm
       mock-update-tpk-niputuspvm]
      (let [event (tu/mock-handler-event :scheduledherate)
            context (tu/mock-handler-context 40000)
            results [{:type "mock-update-tpk-niputuspvm"
                      :jakso-id 863
                      :new-value "ei_niputeta"}

                     {:type "mock-get-existing-nippu"
                      :jakso (get-specific-niputtamaton 1)}
                     {:type "mock-save-tpk-nippu"
                      :nippu (tpk/create-tpk-nippu
                               (get-specific-niputtamaton 1))}
                     {:type "mock-update-tpk-niputuspvm"
                      :jakso-id 1
                      :new-value (str (LocalDate/now))}

                     ;; Memoized-nipun tulokset
                     {:type "mock-update-tpk-niputuspvm"
                      :jakso-id 2
                      :new-value (str (LocalDate/now))}

                     ;; Olemassa olevan nipun tulokset
                     {:type "mock-get-existing-nippu"
                      :jakso (get-specific-niputtamaton 3)}
                     {:type "mock-update-tpk-niputuspvm"
                      :jakso-id 1234
                      :new-value "2021-12-15"}]]
        (tpk/-handleTpkNiputus {} event context)
        (is (= results (vec (reverse @mock-handleTpkNiputus-results))))))))
