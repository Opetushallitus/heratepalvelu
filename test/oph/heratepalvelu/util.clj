(ns oph.heratepalvelu.util
  (:require [clojure.test :refer :all]))

(defn mock-gets [url & [options]]
  (cond
    (.endsWith url "/opiskeluoikeus/1.2.246.562.15.43634207518")
    {:status 200
     :body {:oid "1.2.246.562.15.43634207518"
            :suoritukset
                 [{:tyyppi {:koodiarvo "ammattitutkinto"}
                   :suorituskieli {:koodiarvo "FI"}
                   :koulustusmoduuli {:tunniste {:koodiarvo 123456}}}]
            :koulutustoimija {:oid "1.2.246.562.10.346830761110"}
            :oppilaitos {:oid "1.2.246.562.10.52251087186"}}}
    (.endsWith url "/opiskeluoikeus/1.2.246.562.15.43634207512")
    {:status 200
     :body {:oid "1.2.246.562.15.43634207512"
            :suoritukset
                 [{:tyyppi {:koodiarvo "ammattitutkinto"}
                   :suorituskieli {:koodiarvo "FI"}
                   :koulustusmoduuli {:tunniste {:koodiarvo 123456}}}]
            :oppilaitos {:oid "1.2.246.562.10.52251087186"}}}))

(defn mock-get-organisaatio [oid]
  (cond
    (= oid "1.2.246.562.10.52251087186")
    {:parentOid "1.2.246.562.10.346830761110"}))

(defn mock-posts [url & [options]]
  (cond
    (.endsWith url "/api/vastauslinkki/v1")
    {:kysely_linkki "https://arvovastaus.csc.fi/ABC123"}))

(defn mock-cas-posts [url body & [options]])

(defn mock-get-item-from-whitelist [conds table]
  (cond
    (= "1.2.246.562.10.346830761110"
       (last (:organisaatio-oid conds)))
    {:organisaatio-oid "1.2.246.562.10.346830761110"
     :kayttoonottopvm "2019-07-01"}
    (= "1.2.246.562.10.346830761110"
       (last (:organisaatio-oid conds)))
    {:organisaatio-oid "1.2.246.562.10.346830761111"
     :kayttoonottopvm "3019-07-01"}))
