(ns oph.heratepalvelu.util.date
  (:import (java.time DayOfWeek LocalDate))
  (:refer-clojure :exclude [range]))

(defn latest
  [& dates]
  (reduce #(let [d (cast LocalDate %2)]
             (if (pos? (compare % d)) % d)) dates))

(defn earliest
  [& dates]
  (reduce #(let [d (cast LocalDate %2)]
             (if (neg? (compare % d)) % d)) dates))

(defn range
  "Rakentaa laiskan sekvenssin päivämääristä alkupäivämäärän `start` ja
  loppupäivämäärän `end` perusteella. `start` ja `end` kuuluvat mukaan
  sekvenssiin."
  [start end]
  (let [end+1 (.plusDays ^LocalDate end 1)]
    (take-while #(.isBefore ^LocalDate % end+1)
                (iterate #(.plusDays ^LocalDate % 1) start))))

(defn weekday?
  "Tarkistaa, onko annettu päivämäärä arkipäivä."
  [^LocalDate date]
  (not (or (= (.getDayOfWeek date) DayOfWeek/SATURDAY)
           (= (.getDayOfWeek date) DayOfWeek/SUNDAY))))
