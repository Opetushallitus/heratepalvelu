(ns oph.heratepalvelu.external.organisaatio-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.external.organisaatio :as org]
            [oph.heratepalvelu.test-util :as tu])
  (:import (clojure.lang ExceptionInfo)))

(use-fixtures :once tu/clear-logs-before-test)

(defn- mock-client-get [^String uri options]
  (if (.contains uri "11111")
    (throw (ex-info "Random error" {}))
    {:body {:uri uri :options options}}))

(deftest test-get-organisaatio
  (testing "get-organisaatio tekee kutsuja ja virhekäsittelyä oikein"
    (with-redefs [clojure.tools.logging/log* tu/mock-log*
                  environ.core/env {:organisaatio-url "example.com/org/"}
                  oph.heratepalvelu.external.http-client/get mock-client-get]
      (let [expected {:uri "example.com/org/12345" :options {:as :json}}
            expected-log-regex #"(?s)Virhe hausta organisaatiopalvelusta:.+"]
        (is (= (org/get-organisaatio 12345) expected))
        (is (thrown? ExceptionInfo (org/get-organisaatio 11111)))
        (is (tu/logs-contain-matching? :error expected-log-regex))))))
