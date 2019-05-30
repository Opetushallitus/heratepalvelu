(ns oph.heratepalvelu.external.http-client
  (:refer-clojure :exclude [get])
  (:require [clj-http.client :as client]
            [environ.core :refer [env]]))

(def client-options
  {:headers {"Caller-Id" (:caller-id env)}})

(defn get [url & [options]]
  (client/get url (merge client-options options)))

(defn post [url & [options]]
  (client/post url (merge client-options options)))
