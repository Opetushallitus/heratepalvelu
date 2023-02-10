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

(defn- mock-check-opiskeluoikeus-suoritus-types? [_]
  (reset! call-log (str @call-log "check-opiskeluoikeus-suoritus-types? "))
  true)

(defn- mock-check-organisaatio-whitelist? [_ _]
  (reset! call-log (str @call-log "check-organisaatio-whitelist? "))
  true)

(defn- mock-check-sisaltyy-opiskeluoikeuteen? [_]
  (reset! call-log (str @call-log "check-sisaltyy-opiskeluoikeuteen? "))
  true)

(defn- mock-save-herate [herate opiskeluoikeus koulutustoimija herate-source]
  (reset! results {:herate herate
                   :opiskeluoikeus opiskeluoikeus
                   :koulutustoimija koulutustoimija
                   :herate-source herate-source}))

(deftest test-handleAMISherate
  (testing "Varmista, että -handleAMISherate toimii oikein"
    (with-redefs [oph.heratepalvelu.amis.AMISCommon/save-herate mock-save-herate
                  oph.heratepalvelu.common/get-koulutustoimija-oid
                  mock-get-koulutustoimija-oid
                  oph.heratepalvelu.common/check-opiskeluoikeus-suoritus-types?
                  mock-check-opiskeluoikeus-suoritus-types?
                  oph.heratepalvelu.common/check-organisaatio-whitelist?
                  mock-check-organisaatio-whitelist?
                  oph.heratepalvelu.common/check-sisaltyy-opiskeluoikeuteen?
                  mock-check-sisaltyy-opiskeluoikeuteen?
                  oph.heratepalvelu.external.koski/get-opiskeluoikeus-catch-404!
                  mock-get-opiskeluoikeus-catch-404]
      (let [event (tu/mock-sqs-event {:alkupvm "2021-10-10"
                                      :opiskeluoikeus-oid "1234.5.6678"
                                      :oppija-oid "12345"
                                      :kyselytyyppi "aloittaneet"})
            context (tu/mock-handler-context)]
        (with-redefs [oph.heratepalvelu.common/check-valid-herate-date
                      mock-check-valid-herate-date-true]
          (hh/-handleAMISherate {} event context)
          (is (= @call-log (str "get-opiskeluoikeus-catch-404 "
                                "get-koulutustoimija-oid "
                                "check-opiskeluoikeus-suoritus-types? "
                                "check-organisaatio-whitelist? "
                                "check-sisaltyy-opiskeluoikeuteen? ")))
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
        (with-redefs [oph.heratepalvelu.common/check-valid-herate-date
                      mock-check-valid-herate-date-false]
          (hh/-handleAMISherate {} event context)
          (is (= @call-log ""))
          (is (= @results {})))))))
