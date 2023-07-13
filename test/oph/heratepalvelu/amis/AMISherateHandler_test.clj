(ns oph.heratepalvelu.amis.AMISherateHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.amis.AMISherateHandler :as hh]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.test-util :as tu]))

(def call-log (atom ""))
(def results (atom {}))

(defn- mock-check-valid-herate-date-true [_] true)
(defn- mock-check-valid-herate-date-false [_] false)

(defn- mock-get-opiskeluoikeus-catch-404 [opiskeluoikeus-oid]
  (reset! call-log (str @call-log "get-opiskeluoikeus-catch-404 "))
  {:opiskeluoikeus-oid opiskeluoikeus-oid
   :koulutustoimija-oid "1234"})

(defn- mock-get-koulutustoimija-oid [opiskeluoikeus]
  (reset! call-log (str @call-log "get-koulutustoimija-oid "))
  (:koulutustoimija-oid opiskeluoikeus))

(defn- mock-has-one-or-more-ammatillinen-tutkinto? [_]
  (reset! call-log (str @call-log "has-one-or-more-ammatillinen-tutkinto? "))
  true)

(defn- mock-whitelisted-organisaatio?! [_ _]
  (reset! call-log (str @call-log "whitelisted-organisaatio?! "))
  true)

(defn- mock-sisaltyy-toiseen-opiskeluoikeuteen? [_]
  (reset! call-log (str @call-log "sisaltyy-toiseen-opiskeluoikeuteen? "))
  false)

(defn- mock-save-herate [herate opiskeluoikeus koulutustoimija herate-source]
  (reset! results {:herate herate
                   :opiskeluoikeus opiskeluoikeus
                   :koulutustoimija koulutustoimija
                   :herate-source herate-source}))

(deftest test-handleAMISherate
  (testing "Varmista, että -handleAMISherate toimii oikein"
    (with-redefs [oph.heratepalvelu.amis.AMISCommon/check-and-save-herate!
                  mock-save-herate

                  c/get-koulutustoimija-oid mock-get-koulutustoimija-oid

                  c/has-one-or-more-ammatillinen-tutkinto?
                  mock-has-one-or-more-ammatillinen-tutkinto?

                  c/whitelisted-organisaatio?! mock-whitelisted-organisaatio?!

                  c/sisaltyy-toiseen-opiskeluoikeuteen?
                  mock-sisaltyy-toiseen-opiskeluoikeuteen?

                  oph.heratepalvelu.external.koski/get-opiskeluoikeus-catch-404!
                  mock-get-opiskeluoikeus-catch-404]
      (let [event (tu/mock-sqs-event {:alkupvm "2021-10-10"
                                      :opiskeluoikeus-oid "1234.5.6678"
                                      :oppija-oid "12345"
                                      :kyselytyyppi "aloittaneet"})
            context (tu/mock-handler-context)]
        (with-redefs [c/valid-herate-date?
                      mock-check-valid-herate-date-true]
          (hh/-handleAMISherate {} event context)
          (is (= @call-log (str "get-opiskeluoikeus-catch-404 "
                                "get-koulutustoimija-oid "
                                "has-one-or-more-ammatillinen-tutkinto? "
                                "whitelisted-organisaatio?! "
                                "sisaltyy-toiseen-opiskeluoikeuteen? ")))
          (is (= @results {:herate {:alkupvm "2021-10-10"
                                    :opiskeluoikeus-oid "1234.5.6678"
                                    :oppija-oid "12345"
                                    :kyselytyyppi "aloittaneet"}
                           :opiskeluoikeus {:opiskeluoikeus-oid "1234.5.6678"
                                            :koulutustoimija-oid "1234"}
                           :koulutustoimija "1234"
                           :herate-source (:ehoks c/herate-sources)})))
        (reset! call-log "")
        (reset! results {})
        (with-redefs [c/valid-herate-date?
                      mock-check-valid-herate-date-false]
          (hh/-handleAMISherate {} event context)
          (is (= @call-log
                 "get-opiskeluoikeus-catch-404 get-koulutustoimija-oid "))
          (is (= @results {})))))))
