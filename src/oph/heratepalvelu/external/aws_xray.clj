(ns oph.heratepalvelu.external.aws-xray
  (:import (com.amazonaws.xray AWSXRay)))

(defn wrap-aws-xray [url request]
  (let [segment (AWSXRay/beginSubsegment "HTTP GET")]
    (try
      (let [resp (request)]
        (.putHttp segment "request" {"method" "GET"
                                     "url" url})
        (.putHttp segment "response"
                  {"status" (:status resp)
                   "content_length"
                            (get-in resp [:headers "content-length"] 0)})
        resp)
      (catch Exception e
        (.addException segment e)
        (throw e))
      (finally
        (AWSXRay/endSubsegment)))))