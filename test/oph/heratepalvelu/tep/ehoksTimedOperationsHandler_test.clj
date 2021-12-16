(ns oph.heratepalvelu.tep.ehoksTimedOperationsHandler-test
  (:require [clojure.string :as s]
            [clojure.test :refer :all]
            [oph.heratepalvelu.tep.ehoksTimedOperationsHandler :as etoh]
            [oph.heratepalvelu.test-util :as tu])
  (:import (java.time LocalDate)))

(def results (atom ""))

(defn- mock-get-paattyneet-tyoelamajaksot [start end limit]
  (reset! results (str @results start " " end " "))
  {:body {:data limit}})

;; https://stackoverflow.com/a/41823278
(defn- mock-log* [logger level throwable msg]
  (reset! results (str @results (if (= level :info) "INFO" "OTHER") ": " msg)))

(deftest test-handleTimedOperations
  (testing "Varmista, että -handleTimedOperations kutsuu ehoks-palvelun oikein"
    (with-redefs [clj-time.core/today (fn [] (LocalDate/of 2021 10 10))
                  clojure.tools.logging/log* mock-log*
                  oph.heratepalvelu.external.ehoks/get-paattyneet-tyoelamajaksot
                  mock-get-paattyneet-tyoelamajaksot]
      (let [event (tu/mock-handler-event :scheduledherate)
            context (tu/mock-handler-context)
            expected (str "INFO: Käynnistetään jaksojen lähetys"
                          "2021-07-01 2021-10-10 "
                          "INFO: Lähetetty  100  viestiä")]
        (etoh/-handleTimedOperations {} event context)
        (is (= @results expected))))))
