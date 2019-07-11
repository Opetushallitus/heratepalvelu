(ns oph.heratepalvelu.external.http-client
  (:refer-clojure :exclude [get])
  (:require [clj-http.client :as client]
            [environ.core :refer [env]]
            [oph.heratepalvelu.external.aws-xray :refer [wrap-aws-xray]]))

(def client-options
  {:headers {"Caller-Id" (:caller-id env)}})

(defn get [url & [options]]
  (wrap-aws-xray url
                 #(client/get url (merge client-options options))))

(defn post [url & [options]]
  (wrap-aws-xray url
                 #(client/post url (merge client-options options))))
