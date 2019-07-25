(ns oph.heratepalvelu.external.koski
  (:require [oph.heratepalvelu.external.http-client :as client]
            [environ.core :refer [env]]
            [clojure.tools.logging :as log]))

(defn get-opiskeluoikeus [oid]
  (:body (client/get (str (:koski-url env) "opiskeluoikeus/" oid)
                      {:basic-auth [(:koski-user env) (:koski-pwd env)]
                       :as :json})))

(defn get-updated-opiskeluoikeudet [ts]
  [])
