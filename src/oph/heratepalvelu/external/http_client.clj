(ns oph.heratepalvelu.external.http-client
  (:refer-clojure :exclude [get])
  (:require [clj-http.client :as client]
            [environ.core :refer [env]]
            [oph.heratepalvelu.external.aws-xray :refer [wrap-aws-xray]]))

(def client-options
  {:headers {"Caller-Id" (str "1.2.246.562.10.00000000001." (:caller-id env))}})

(defn- merge-options [options]
  (merge (assoc client-options :headers
                               (merge (:headers client-options)
                                      (:headers options)))
         (dissoc options :headers)))

(defn get [url & [options]]
  (wrap-aws-xray url :get
                 #(client/get url (merge-options options))))

(defn post [url & [options]]
  (wrap-aws-xray url :post
                 #(client/post url (merge-options options))))

(defn delete [url & [options]]
  (wrap-aws-xray url :delete
                 #(client/delete url (merge-options options))))

(defn patch [url & [options]]
  (wrap-aws-xray url :patch
                 #(client/patch url (merge-options options))))
