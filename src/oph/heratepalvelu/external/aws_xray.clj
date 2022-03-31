(ns oph.heratepalvelu.external.aws-xray
  (:require [clojure.string :as str])
  (:import (com.amazonaws.xray AWSXRay)))

(defn- wrap-begin-subsegment [line] (AWSXRay/beginSubsegment line))

(defn- wrap-end-subsegment [] (AWSXRay/endSubsegment))

(defn wrap-aws-xray
  "Käärii requestin X-Rayiin."
  [url method request]
  (let [segment (wrap-begin-subsegment
                  (str "HTTP " (str/upper-case (name method))))]
    (try
      (.putHttp segment "request" {"method" (str/upper-case (name method))
                                   "url" url})
      (let [resp (request)]
        (.putHttp segment "response"
                  {"status" (:status resp)
                   "content_length"
                   (get-in resp [:headers "Content-Length"] 0)})
        resp)
      (catch Exception e
        (.addException segment e)
        (throw e))
      (finally
        (wrap-end-subsegment)))))
