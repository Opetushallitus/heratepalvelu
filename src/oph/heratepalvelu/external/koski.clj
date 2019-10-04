(ns oph.heratepalvelu.external.koski
  (:require [oph.heratepalvelu.external.http-client :as client]
            [environ.core :refer [env]]))

(defn get-opiskeluoikeus [oid]
  (:body (client/get (str (:koski-url env) "opiskeluoikeus/" oid)
                      {:basic-auth [(:koski-user env) (:koski-pwd env)]
                       :as :json})))

(defn get-updated-opiskeluoikeudet [datetime-str page]
  (let
    [resp
     (client/get
       (str (:koski-url env) "oppija/")
       {:query-params {"opiskeluoikeudenTyyppi" "ammatillinenkoulutus"
                       "muuttunutJälkeen" datetime-str
                       "pageSize" 100
                       "pageNumber" page}
                          :basic-auth [(:koski-user env) (:koski-pwd env)]
                          :as :json-strict})]
    (sort-by :aikaleima
             (reduce
               #(into %1 (:opiskeluoikeudet %2))
               []
               (:body resp)))))
