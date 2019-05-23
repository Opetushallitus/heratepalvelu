(ns oph.heratepalvelu.external.organisaatio
  (:require [oph.heratepalvelu.external.http-client :as client]
            [environ.core :refer [env]]
            [clojure.tools.logging :as log]))

(defn get-organisaatio [oid]
  (:body (client/get (str (:organisaatio-url env) oid) {:as :json})))
