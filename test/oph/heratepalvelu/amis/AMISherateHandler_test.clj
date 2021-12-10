(ns oph.heratepalvelu.amis.AMISherateHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.amis.AMISherateHandler :as hh]))

(defn- mock-check-valid-herate-date-true [date] true)
(defn- mock-check-valid-herate-date-false [date] false)
