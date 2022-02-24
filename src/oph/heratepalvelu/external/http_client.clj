(ns oph.heratepalvelu.external.http-client
  (:refer-clojure :exclude [get])
  (:require [clj-http.client :as client]
            [environ.core :refer [env]]
            [oph.heratepalvelu.external.aws-xray :refer [wrap-aws-xray]]))

(def client-options
  {:headers {"Caller-Id" (:caller-id env)
             "CSRF" (:caller-id env)}
   :cookies {"CSRF" {:value (:caller-id env)
                     :path "/"}}})

(defn merge-options
  "Muuttaa headersit default optioiden headers-kenttään ja yhdistää default
  optiot ja kutsujan antamat optiot."
  [options]
  (merge (assoc client-options :headers
                               (merge (:headers client-options)
                                      (:headers options)))
         (dissoc options :headers)))

(defn get
  "Tekee GET-kyselyn X-Rayn clj-http clientin kautta."
  [url & [options]]
  (wrap-aws-xray url :get
                 #(client/get url (merge-options options))))

(defn post
  "Tekee POST-kyselyn X-Rayn clj-http clientin kautta."
  [url & [options]]
  (wrap-aws-xray url :post
                 #(client/post url (merge-options options))))

(defn delete
  "Tekee DELETE-kyselyn X-Rayn clj-http clientin kautta."
  [url & [options]]
  (wrap-aws-xray url :delete
                 #(client/delete url (merge-options options))))

(defn patch
  "Tekee PATCH-kyselyn X-Rayn clj-http clientin kautta."
  [url & [options]]
  (wrap-aws-xray url :patch
                 #(client/patch url (merge-options options))))
