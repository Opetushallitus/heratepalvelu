(ns oph.heratepalvelu.external.aws-xray
  "Wrapperit X-Rayn ympäri."
  (:require [clojure.string :as str])
  (:import (com.amazonaws.xray AWSXRay)
           (com.amazonaws.xray.entities Segment)))

(defn- wrap-begin-subsegment
  "Wrapper AWSXRayn beginSubsegmentin ympäri."
  ^Segment [line]
  (AWSXRay/beginSubsegment line))

(defn- wrap-end-subsegment
  "Wrapper AWSXRayn endSubsegmentin ympäri."
  []
  (AWSXRay/endSubsegment))

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
