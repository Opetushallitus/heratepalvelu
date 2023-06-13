(ns oph.heratepalvelu.util.string-test
  (:require
   [clojure.test :refer [are deftest testing]]
   [oph.heratepalvelu.util.string :as s]))

(deftest test-first-and-last
  (testing "`first` ja `last` palauttavat ensimmäisen ja viimeisen
            merkkijonon sanakirjajärjestyksessä."
    (are [first-str last-str strs] (and (= (apply s/first strs) first-str)
                                        (= (apply s/last  strs) last-str))
         "c"          "h"          ["e" "h" "d" "f" "c"]
         "0x"         "b"          ["a" "1" "b" "123" "0x" "@"]
         "Mehukatti"  "mehukatti"  ["kissa" "koira" "mehukatti" "Mehukatti"]
         "2021-03-05" "2023-01-01" ["2022-01-04" "2021-03-05" "2023-01-01"])))
