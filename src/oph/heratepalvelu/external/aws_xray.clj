(ns oph.heratepalvelu.external.aws-xray
  (:require [clojure.string :as str])
  (:import (com.amazonaws.xray AWSXRay)))

(defn wrap-aws-xray [url method request]
  (let [segment (AWSXRay/beginSubsegment
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
        (AWSXRay/endSubsegment)))))