(ns oph.heratepalvelu.tpk.tpkNiputusHandler-test
  (:require [clj-time.core :as t]
            [clojure.test :refer :all]
            [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.tpk.tpkNiputusHandler :as tpk]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

;; query-niputtamattomat -funktiota ei tällä hetkellä testata, koska mockien
;; kirjoittaminen Java-luokkiin näyttää olevan kohtuuttoman vaikea (ainakin
;; ilman muita kirjastoja). Toivottavasti joskus faktoroidaan scan-logiikka
;; pois, mikä sallii projektin sisäisen scan-funktion mockaamisen.

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

(deftest test-get-kausi-alkupvm-loppupvm
  (testing "get-kausi-alkupvm luo oikean alkupäivämäärän ja loppupäivämäärän"
    (let [jakso1 {:koulutustoimija        "1.2.246.562.10.346830761110"
                  :tyopaikan_nimi         "Testi työpaikka"
                  :tyopaikan_ytunnus      "1234567-8"
                  :jakso_loppupvm         "2021-11-20"
                  :hankkimistapa_tyyppi   "koulutussopimus"}
          jakso2 {:koulutustoimija        "1.2.246.562.10.346830761110"
                  :tyopaikan_nimi         "Testi työpaikka"
                  :tyopaikan_ytunnus      "1234567-8"
                  :jakso_loppupvm         "2022-03-09"
                  :hankkimistapa_tyyppi   "koulutussopimus"}]
      (is (= "2021-07-01" (tpk/get-kausi-alkupvm jakso1)))
      (is (= "2022-01-01" (tpk/get-kausi-alkupvm jakso2)))
      (is (= "2021-12-31" (tpk/get-kausi-loppupvm jakso1)))
      (is (= "2022-06-30" (tpk/get-kausi-loppupvm jakso2))))))

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

(deftest test-create-nippu
  (testing "Varmistaa, että create-nippu luo niput oikein"
    (let [jakso {:koulutustoimija        "1.2.246.562.10.346830761110"
                 :tyopaikan_nimi         "Testi työpaikka"
                 :tyopaikan_ytunnus      "1234567-8"
                 :jakso_loppupvm         "2021-11-20"
                 :hankkimistapa_tyyppi   "koulutussopimus"}]
      (is (= (tpk/create-nippu jakso "abcde")
             {:nippu-id
              "testi_tyopaikka/1234567-8/1.2.246.562.10.346830761110/2021-07-01_2021-12-31"
              :tyopaikan-nimi               "Testi työpaikka"
              :tyopaikan-nimi-normalisoitu  "testi_tyopaikka"
              :vastaamisajan-alkupvm        "2022-01-15"
              :vastaamisajan-loppupvm       "2022-02-28"
              :tyopaikan-ytunnus            "1234567-8"
              :koulutustoimija-oid          "1.2.246.562.10.346830761110"
              :tiedonkeruu-alkupvm          "2021-07-01"
              :tiedonkeruu-loppupvm         "2021-12-31"
              :niputuspvm                   (str (t/today))
              :request-id                   "abcde"})))))

(deftest test-extend-nippu
  (testing "Varmistaa, että extend-nippu lisää kenttiä nippuun oikein"
    (let [nippu {:nippu-id "test-id"}
          arvo-resp {:kysely_linkki "kysely.linkki/123"
                     :tunnus "QWERTY"
                     :voimassa_loppupvm "2021-12-12"}
          expected {:nippu-id "test-id"
                    :kyselylinkki "kysely.linkki/123"
                    :tunnus "QWERTY"
                    :voimassa-loppupvm "2021-12-12"}]
      (is (= (tpk/extend-nippu nippu arvo-resp) expected)))))

(deftest test-make-arvo-request
  (testing "Varmistaa, että make-arvo-request tekee kutsuja oikein"
    (with-redefs [oph.heratepalvelu.external.arvo/build-tpk-request-body
                  (fn [nippu] {:test-field-body (:test-field-nippu nippu)})
                  oph.heratepalvelu.external.arvo/create-tpk-kyselylinkki
                  (fn [body] {:test-field (:test-field-body body)
                              :kyselylinkki "kysely.linkki/123"
                              :tunnus "QWERTY"
                              :voimassa-loppupvm "2021-12-12"})]
      (let [nippu {:test-field-nippu "test-field"
                   :nippu-id "test-id"}
            expected {:test-field "test-field"
                      :kyselylinkki "kysely.linkki/123"
                      :tunnus "QWERTY"
                      :voimassa-loppupvm "2021-12-12"}]
        (is (= (tpk/make-arvo-request nippu) expected))))))

(deftest test-get-existing-nippu
  (testing "Varmistaa, että get-existing-nippu kutsuu get-item oikein"
    (with-redefs [environ.core/env {:tpk-nippu-table "tpk-nippu-table-name"}
                  oph.heratepalvelu.db.dynamodb/get-item
                  (fn [query-params table]
                    (when (and (= :s (first (:nippu-id query-params)))
                               (= :s (first
                                       (:tiedonkeruu-alkupvm query-params)))
                               (= table "tpk-nippu-table-name"))
                      {:nippu-id (second (:nippu-id query-params))
                       :tiedonkeruu-alkupvm (second (:tiedonkeruu-alkupvm
                                                      query-params))}))]
      (let [jakso {:tyopaikan_nimi "Ääkköset Által"
                   :tyopaikan_ytunnus "123456-7"
                   :koulutustoimija "test-kt-id"
                   :jakso_loppupvm "2021-10-10"}
            expected {:nippu-id (str "aakkoset_altal/123456-7/test-kt-id/"
                                     "2021-07-01_2021-12-31")
                      :tiedonkeruu-alkupvm "2021-07-01"}]
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
             (= :s (first (:request-id query-params)))
             (= :s (first (:kyselylinkki query-params)))
             (= :s (first (:tunnus query-params)))
             (= :s (first (:voimassa-loppupvm query-params)))
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
             :niputuspvm (second (:niputuspvm query-params))
             :request-id (second (:request-id query-params))
             :kyselylinkki (second (:kyselylinkki query-params))
             :tunnus (second (:tunnus query-params))
             :voimassa-loppupvm (second (:voimassa-loppupvm query-params))})))

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
                   :niputuspvm "2021-12-15"
                   :request-id "1234567"
                   :kyselylinkki "kysely.linkki/132"
                   :tunnus "QWERTY"
                   :voimassa-loppupvm "2022-03-01"}]
        (tpk/save-nippu nippu)
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

(definterface IMockScanResponse
  (items [])
  (hasLastEvaluatedKey []))

(deftype MockScanResponse [items-to-return]
  IMockScanResponse
  (items [this] items-to-return)
  (hasLastEvaluatedKey [this] false))

(defn- mock-query-niputtamattomat [exclusive-start-key]
  (MockScanResponse.
    (map ddb/map-vals-to-attribute-values mock-niputtamattomat-list)))

(defn- mock-get-existing-nippu [jakso]
  (append-to-mock-handleTpkNiputus-results {:type "mock-get-existing-nippu"
                                            :jakso jakso})
  (when (= 1234 (:hankkimistapa_id jakso)) {:niputuspvm "2021-12-15"}))

(defn- mock-make-arvo-request [nippu]
  (append-to-mock-handleTpkNiputus-results {:type "mock-make-arvo-request"
                                            :nippu (dissoc nippu :request-id)})
  {:kysely_linkki "kysely.linkki/123"
   :tunnus "ASDFGH"
   :voimassa_loppupvm "2022-01-01"})

(defn- mock-save-nippu [nippu]
  (append-to-mock-handleTpkNiputus-results {:type "mock-save-nippu"
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
       oph.heratepalvelu.tpk.tpkNiputusHandler/make-arvo-request
       mock-make-arvo-request
       oph.heratepalvelu.tpk.tpkNiputusHandler/query-niputtamattomat
       mock-query-niputtamattomat
       oph.heratepalvelu.tpk.tpkNiputusHandler/save-nippu mock-save-nippu
       oph.heratepalvelu.tpk.tpkNiputusHandler/update-tpk-niputuspvm
       mock-update-tpk-niputuspvm]
      (let [event (tu/mock-handler-event :scheduledherate)
            context (tu/mock-handler-context 40000)
            results [{:type "mock-update-tpk-niputuspvm"
                      :jakso-id 863
                      :new-value "ei_niputeta"}

                     {:type "mock-get-existing-nippu"
                      :jakso (get-specific-niputtamaton 1)}
                     {:type "mock-make-arvo-request"
                      :nippu (dissoc
                               (tpk/create-nippu (get-specific-niputtamaton 1)
                                                 "asdf")
                               :request-id)}
                     {:type "mock-save-nippu"
                      :nippu (assoc
                               (dissoc
                                 (tpk/create-nippu (get-specific-niputtamaton 1)
                                                   "asdf")
                                 :request-id)
                               :kyselylinkki "kysely.linkki/123"
                               :tunnus "ASDFGH"
                               :voimassa-loppupvm "2022-01-01")}
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
