(ns oph.heratepalvelu.db.dynamodb
  (:require [taoensso.faraday :as ddb]
            [environ.core :refer [env]]))

(def client-opts
  { :endpoint (str "http://dynamodb." (:region env) ".amazonaws.com") })

(defn put-item
  [item options]
  (ddb/put-item client-opts (:herate-table env) item options))

(defn query-items
  [prim-key-conds options]
  (ddb/query client-opts (:herate-table env) prim-key-conds options))

(defn update-item [prim-key-conds options]
  (ddb/update-item client-opts (:herate-table env) prim-key-conds options))
