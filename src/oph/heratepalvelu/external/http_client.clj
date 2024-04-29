(ns oph.heratepalvelu.external.http-client
  "Wrapperit HTTP-clientin ympäri."
  (:refer-clojure :exclude [get])
  (:require [clj-http.client :as client]
            [environ.core :refer [env]]
            [clojure.tools.logging :as log]
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

(def temporary-conditions
  #{java.net.SocketTimeoutException java.net.ConnectException
    ; these should not be temporary but often are
    400 404 500
    ; these are allowed to be temporary
    408 425 429 502 503 504
    ; these are non-standard extensions; probably doesn't do harm to include
    520 521 522 524 529 598 599})

(defn temporary-error?
  "Kertoo poikkeuksesta, voiko sen aiheuttaja olla väliaikainen."
  [e]
  (contains? temporary-conditions (or (:status (ex-data e)) (type e))))

(defn request-with-retries
  "Tekee HTTP-kutsun enintään 3 kertaa siten, että samaa kutsua yritetään
  uudestaan, mikäli se epäonnistuu tavalla joka saattaa olla väliaikainen.
  Raportoi epäonnistuneet kutsut."
  [options]
  (loop [retries 2]
    (if (or (:no-retries options) (<= retries 0))
      (try (client/request options)
           (catch Exception e
             (log/error e "error" (ex-data e)
                        "for request" options ", not retrying")
             (throw e)))
      (let [result (try (client/request options) (catch Exception e e))]
        (cond
          (not (instance? Exception result)) result

          (temporary-error? result)
          (do (log/info "Got temporary error" (:status (ex-data result))
                        "/" (.getName ^Class (type result))
                        "; retrying" retries "times")
              (recur (dec retries)))

          :else
          (do (log/error result "error" (ex-data result) "for request" options)
              (throw result)))))))

(defn request
  "Tekee HTTP-kutsun X-Rayn avulla."
  [options]
  (let [real-request #(request-with-retries (merge-options options))]
    (if (env :disable-aws-xray)
      (real-request)
      (wrap-aws-xray (:url options) (:method options) real-request))))

(defn method-function
  "Luo HTTP-kyselyfunktion jollekin tietylle HTTP-verbille."
  [method]
  (fn [url & [options]]
    (request (merge {:url url :method method} options))))

(def get (method-function :get))
(def post (method-function :post))
(def delete (method-function :delete))
(def patch (method-function :patch))
