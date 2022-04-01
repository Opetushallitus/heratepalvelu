(ns oph.heratepalvelu.tpk.tpkCommon
  "Yhteiset funktiot TPK-toiminnallisuuteen."
  (:require [oph.heratepalvelu.common :as c])
  (:import (java.time LocalDate)))

(defn get-kausi-alkupvm
  "Laske alkupäivämäärän tiedonkeruukaudelle, johon päivämäärä kuuluu."
  [pvm]
  (LocalDate/of (.getYear pvm) (if (<= (.getMonthValue pvm) 6) 1 7) 1))

(defn get-kausi-loppupvm
  "Laske loppupäivämäärän tiedonkeruukaudelle, johon päivämäärä kuuluu."
  [pvm]
  (if (<= (.getMonthValue pvm) 6)
    (LocalDate/of (.getYear pvm) 6 30)
    (LocalDate/of (.getYear pvm) 12 31)))

(defn get-current-kausi-alkupvm
  "Laske alkupäivämäärän edeltävälle tiedonkeruukaudelle, jos vastausaika ei ole
  vielä umpeutunut; muuten seuraavalle tiedonkeruukaudelle. Esim.:
  02.02.2022 -> 01.07.2021
  03.03.2022 -> 01.01.2022"
  []
  (get-kausi-alkupvm (.minusMonths (c/local-date-now) 2)))

(defn get-current-kausi-loppupvm
  "Laske alkupäivämäärän edeltävälle tiedonkeruukaudelle, jos vastausaika ei ole
  vielä umpeutunut; muuten seuraavalle tiedonkeruukaudelle. Esim.:
  02.02.2022 -> 01.07.2021
  03.03.2022 -> 01.01.2022"
  []
  (get-kausi-loppupvm (.minusMonths (c/local-date-now) 2)))
