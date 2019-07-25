(ns oph.heratepalvelu.external.ehoks
  (:require [oph.heratepalvelu.external.cas-client :refer [cas-authenticated-get]]
            [environ.core :refer [env]]))

(defn get-hoks-by-opiskeluoikeus [opiskeluoikeus-oid]
  (:body (cas-authenticated-get
           (str (:ehoks-url env) "hoks/opiskeluoikeus/" opiskeluoikeus-oid))))