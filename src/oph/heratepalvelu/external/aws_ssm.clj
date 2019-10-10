(ns oph.heratepalvelu.external.aws-ssm
  (:import (software.amazon.awssdk.services.ssm SsmClient)
           (software.amazon.awssdk.regions Region)
           (software.amazon.awssdk.services.ssm.model GetParameterRequest)
           (software.amazon.awssdk.core.client.config ClientOverrideConfiguration)
           (com.amazonaws.xray.interceptors TracingInterceptor)))

(def client (-> (SsmClient/builder)
                (.region (Region/EU_WEST_1))
                (.overrideConfiguration
                  (-> (ClientOverrideConfiguration/builder)
                      (.addExecutionInterceptor (TracingInterceptor.))
                      (.build)))
                (.build)))

(defn get-secret [name]
  (.value
    (.parameter
      (.getParameter client (-> (GetParameterRequest/builder)
                                (.name name)
                                (.withDecryption true)
                                (.build))))))
