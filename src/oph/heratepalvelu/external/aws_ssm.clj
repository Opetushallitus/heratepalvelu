(ns oph.heratepalvelu.external.aws-ssm
  "Wrapperit SSM:n ympäri."
  (:require [environ.core :refer [env]]
            [clojure.string :refer [split]])
  (:import (software.amazon.awssdk.services.ssm SsmClient)
           (software.amazon.awssdk.regions Region)
           (software.amazon.awssdk.services.ssm.model GetParameterRequest)
           (software.amazon.awssdk.core.client.config
             ClientOverrideConfiguration)
           (com.amazonaws.xray.interceptors TracingInterceptor)))

(def ^SsmClient client
  "SSM-client -objekti."
  (-> (SsmClient/builder)
      (.region (Region/EU_WEST_1))
      (.overrideConfiguration
        (-> (ClientOverrideConfiguration/builder)
            (.addExecutionInterceptor (TracingInterceptor.))
            ^ClientOverrideConfiguration (.build)))
      (.build)))

(defn get-secret
  "Hakee salaisen arvon AWS:n SSM-palvelusta tai ympäristömuuttujista
  (jälkemmäinen testausta varten)."
  [secret-name]
  (or (env (keyword (last (split secret-name #"/"))))
      (-> (GetParameterRequest/builder)
          (.name secret-name)
          (.withDecryption true)
          ^GetParameterRequest (.build)
          (->> (.getParameter client))
          (.parameter)
          (.value))))
