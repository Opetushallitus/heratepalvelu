(ns oph.heratepalvelu.tep.ehoksTimedOperationsHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.tep.ehoksTimedOperationsHandler :as etoh]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(use-fixtures :each tu/clear-logs-before-test)

(def results (atom {}))

(defn- mock-get-paattyneet-tyoelamajaksot [start end limit]
  (reset! results {:start start :end end})
  {:body {:data limit}})

(deftest test-handleTimedOperations
  (testing "Varmista, että -handleTimedOperations kutsuu ehoks-palvelun oikein"
    (with-redefs [clojure.tools.logging/log* tu/mock-log*
                  oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2021 10 10))
                  oph.heratepalvelu.external.ehoks/get-paattyneet-tyoelamajaksot
                  mock-get-paattyneet-tyoelamajaksot]
      (let [event (tu/mock-handler-event :scheduledherate)
            context (tu/mock-handler-context)
            expected {:start "2021-07-01" :end "2021-10-10"}]
        (etoh/-handleTimedOperations {} event context)
        (is (= @results expected))
        (is (true? (tu/logs-contain?
                     {:level :info
                      :message "Käynnistetään jaksojen lähetys"})))
        (is (true? (tu/logs-contain?
                     {:level :info
                      :message "Lähetetty 50 viestiä"})))))))
