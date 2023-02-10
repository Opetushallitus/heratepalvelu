(ns oph.heratepalvelu.util.string
  (:refer-clojure :exclude [first last]))

(defn last
  "Palauttaa argumenttina annetuista merkkijonoista sen, joka on
  sanakirjajärjestyksessä viimeinen."
  [& strs]
  (reduce #(let [s (cast String %2)]
             (if (pos? (compare % s)) % s)) strs))

(defn first
  "Palauttaa argumenttina annetuista merkkijonoista sen, joka on
  sanakirjajärjestyksessä ensimmäinen."
  [& strs]
  (reduce #(let [s (cast String %2)]
             (if (neg? (compare % s)) % s)) strs))

