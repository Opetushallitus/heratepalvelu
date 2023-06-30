(ns oph.heratepalvelu.util.date
  (:import (java.time LocalDate))
  (:refer-clojure :exclude [range]))

(defn range
  "Rakentaa laiskan sekvenssin päivämääristä alkupäivämäärän `start` ja
  loppupäivämäärän `end` perusteella. `start` ja `end` kuuluvat mukaan
  sekvenssiin."
  [start end]
  (let [end+1 (.plusDays ^LocalDate end 1)]
    (take-while #(.isBefore ^LocalDate % end+1)
                (iterate #(.plusDays ^LocalDate % 1) start))))
