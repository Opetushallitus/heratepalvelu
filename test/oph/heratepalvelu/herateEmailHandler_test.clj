(ns oph.heratepalvelu.herateEmailHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.herateEmailHandler :as h]
            [clj-time.core :as t]
            [clj-time.format :as f]))

(deftest test-has-time-to-answer
  (let [date1 (t/plus  (t/today) (t/days 10))
        date2 (t/minus (t/today) (t/days 40))
        date3 (t/minus  (t/today) (t/days 29))
        date4 (t/minus (t/today) (t/days 30))]
    (is (= true (h/has-time-to-answer?
                  (f/unparse-local-date (:date f/formatters) date1))))
    (is (= false (h/has-time-to-answer?
                  (f/unparse-local-date (:date f/formatters) date2))))
    (is (= true (h/has-time-to-answer?
                  (f/unparse-local-date (:date f/formatters) date3))))
    (is (= false (h/has-time-to-answer?
                  (f/unparse-local-date (:date f/formatters) date4))))))
