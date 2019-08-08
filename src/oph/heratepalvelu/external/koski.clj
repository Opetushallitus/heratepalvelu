(ns oph.heratepalvelu.external.koski
  (:require [oph.heratepalvelu.external.http-client :as client]
            [environ.core :refer [env]]))

(defn get-opiskeluoikeus [oid]
  (:body (client/get (str (:koski-url env) "opiskeluoikeus/" oid)
                      {:basic-auth [(:koski-user env) (:koski-pwd env)]
                       :as :json})))

(defn get-updated-opiskeluoikeudet [datetime-str]
  (let
    [resp
     (client/get
       (str (:koski-url env) "oppija/")
       {:query-params {"opiskeluoikeudenTyyppi" "ammatillinenkoulutus"
                       ;"opiskeluoikeudenTila" "valmistunut"
                       ; Vahvistettu ei ole välttämättä valmistunut Koskessa,
                       ; muutos tähän olisi hyvä Koskessa
                       "muuttunutJälkeen" datetime-str}
                          :basic-auth [(:koski-user env) (:koski-pwd env)]
                          :as :json-strict})]
    (sort-by :aikaleima
             (reduce
               #(into %1 (:opiskeluoikeudet %2))
               []
               (:body resp)))))
