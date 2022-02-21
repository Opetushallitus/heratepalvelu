(ns oph.heratepalvelu.amis.AMISehoksTimedOperationsHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.amis.AMISehoksTimedOperationsHandler :as etoh]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(use-fixtures :each tu/clear-logs-before-test)

(def results (atom {}))

(defn- mock-get-retry-kyselylinkit [start end limit]
  (reset! results {:start start :end end})
  {:body {:data limit}})

(deftest test-handleAMISTimedOperations
  (testing "Varmista, että -handleAMISTimedOperations toimii oikein"
    (with-redefs [clojure.tools.logging/log* tu/mock-log*
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 2 2))
                  oph.heratepalvelu.external.ehoks/get-retry-kyselylinkit
                  mock-get-retry-kyselylinkit]
      (let [event (tu/mock-handler-event :scheduledherate)
            context (tu/mock-handler-context)
            expected {:start "2021-07-01" :end "2022-02-02"}]
        (etoh/-handleAMISTimedOperations {} event context)
        (is (= @results expected))
        (is (true? (tu/logs-contain?
                     {:level :info
                      :message "Käynnistetään herätteiden lähetys"})))
        (is (true? (tu/logs-contain?
                     {:level :info
                      :message "Lähetetty 100 viestiä"})))))))
