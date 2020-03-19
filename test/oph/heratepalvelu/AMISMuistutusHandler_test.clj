(ns oph.heratepalvelu.AMISMuistutusHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.AMISMuistutusHandler :as m]
            [clj-time.core :as t]
            [clj-time.format :as f]))

(deftest test-has-time-to-answer
  (let [date1 (t/today)
        date2 (t/plus (t/now) (t/days 1))
        date3 (t/minus  (t/now) (t/days 1))]
    (is (true? (m/has-time-to-answer?
                  (str date1))))
    (is (true? (m/has-time-to-answer?
                   (str date2))))
    (is (false? (m/has-time-to-answer?
                  (str date3))))))
