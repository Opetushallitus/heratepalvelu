(ns oph.heratepalvelu.db.dynamodb
  (:require [taoensso.faraday :as ddb]
            [environ.core :refer [env]]
            [clojure.tools.logging :as log]))

(def client-opts
  { :endpoint (str "http://dynamodb." (:region env) ".amazonaws.com") })

(defn put-item [item]
  (ddb/put-item client-opts (:herate-table env) item))

(defn get-item [i]
  (ddb/get-item client-opts (:herate-table env) {}))


