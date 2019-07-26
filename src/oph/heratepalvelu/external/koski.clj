(ns oph.heratepalvelu.external.koski
  (:require [oph.heratepalvelu.external.http-client :as client]
            [environ.core :refer [env]]
            [clojure.tools.logging :as log]))

(defn get-opiskeluoikeus [oid]
  (:body (client/get (str (:koski-url env) "opiskeluoikeus/" oid)
                      {:basic-auth [(:koski-user env) (:koski-pwd env)]
                       :as :json})))

(defn get-updated-opiskeluoikeudet [ts]
  [{
    :oid "1.2.246.562.15.82039738925"
    :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
    :oppilaitos {:oid "1.2.246.562.10.35751498084"}
    :suoritukset [
                  {:suorituskieli {:koodiarvo "FI"}
                   :tyyppi {:koodiarvo "ammatillinentutkinto"}
                   :vahvistus {:päivä "2019-07-24"}}]
    :updated-at (- ts 1000)}
   {
    :oid "1.2.246.562.15.66655788454"
    :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
    :oppilaitos {:oid "1.2.246.562.10.35751498083"}
    :suoritukset [
                  {:suorituskieli {:koodiarvo "FI"}
                   :tyyppi {:koodiarvo "ammatillinentutkinto"}
                   :vahvistus {:päivä "2019-07-24"}}]
    :updated-at (- ts 4000)}
   {
    :oid "1.2.246.562.15.36781389766"
    :koulutustoimija {:oid "1.2.246.562.10.53542906168"}
    :oppilaitos {:oid "1.2.246.562.10.35751498082"}
    :suoritukset [
                  {:suorituskieli {:koodiarvo "FI"}
                   :tyyppi {:koodiarvo "ammatillinentutkinto"}
                   :vahvistus {:päivä "2019-07-24"}}]
    :updated-at (- ts 27000)}
   {
    :oid "1.2.246.562.15.57651618084"
    :koulutustoimija {:oid "1.2.246.562.10.35751498086"}
    :oppilaitos {:oid "1.2.246.562.10.35751498081"}
    :suoritukset [
                  {:suorituskieli {:koodiarvo "FI"}
                   :tyyppi {:koodiarvo "ammatillinentutkinto"}
                   :vahvistus {:päivä "2019-07-24"}}]
    :updated-at (+ ts 24000)}])
