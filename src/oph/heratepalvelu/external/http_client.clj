(ns oph.heratepalvelu.external.http-client
  "Wrapperit HTTP-clientin ympäri."
  (:refer-clojure :exclude [get])
  (:require [clj-http.client :as client]
            [environ.core :refer [env]]
            [oph.heratepalvelu.external.aws-xray :refer [wrap-aws-xray]]))

(def client-options
  "Client-optioiden oletusarvot."
  {:headers {"Caller-Id" (:caller-id env)
             "CSRF"      (:caller-id env)}
   :cookies {"CSRF" {:value (:caller-id env)
                     :path "/"}}})

(defn merge-options
  "Muuttaa headersit default optioiden headers-kenttään ja yhdistää default
  optiot ja kutsujan antamat optiot."
  [options]
  (merge (update-in client-options [:headers] merge (:headers options))
         (dissoc options :headers)))

(defn request
  "Tekee HTTP-kutsun X-Rayn avulla."
  [options]
  (wrap-aws-xray (:url options) (:method options)
                 #(client/request (merge-options options))))

(defn method-function
  "Luo HTTP-kyselyfunktion jollekin tietylle HTTP-verbille."
  [method]
  (fn [url & [options]]
    (request (merge {:url url :method method} options))))

(def get (method-function :get))
(def post (method-function :post))
(def delete (method-function :delete))
(def patch (method-function :patch))
