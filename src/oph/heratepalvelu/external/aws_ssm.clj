(ns oph.heratepalvelu.external.aws-ssm
  (:import (software.amazon.awssdk.services.ssm SsmClient)
           (software.amazon.awssdk.regions Region)
           (software.amazon.awssdk.services.ssm.model GetParameterRequest)
           (software.amazon.awssdk.core.client.config
             ClientOverrideConfiguration)
           (com.amazonaws.xray.interceptors TracingInterceptor)))

(def client (-> (SsmClient/builder)
                (.region (Region/EU_WEST_1))
                (.overrideConfiguration
                  (-> (ClientOverrideConfiguration/builder)
                      (.addExecutionInterceptor (TracingInterceptor.))
                      (.build)))
                (.build)))

(defn- create-get-parameter-request-builder
  "Abstraktio GetParameterRequest/builderin ympäri, joka helpottaa testaamista."
  []
  (GetParameterRequest/builder))

(defn get-secret
  "Hakee salaisen arvon SSMistä."
  [secret-name]
  (.value
    (.parameter
      (.getParameter client (-> (create-get-parameter-request-builder)
                                (.name secret-name)
                                (.withDecryption true)
                                (.build))))))
