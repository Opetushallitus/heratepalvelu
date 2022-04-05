(ns oph.heratepalvelu.external.aws-ssm-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.external.aws-ssm :as ssm])
  (:import (software.amazon.awssdk.services.ssm SsmClient)
           (software.amazon.awssdk.services.ssm.model GetParameterRequest
                                                      GetParameterResponse
                                                      Parameter)))

(def saved-results (atom {}))

(def mockSsmClient
  (proxy [SsmClient] []
    (getParameter [^GetParameterRequest get-param-req]
      (reset! saved-results {:name (.name get-param-req)
                             :withDecryption (.withDecryption get-param-req)})
      (.build (.parameter (GetParameterResponse/builder)
                          ^Parameter (.build (.value (Parameter/builder)
                                                     "asdf")))))))

(deftest test-get-secret
  (testing "Varmista, että get-secret tekee oikeite kutsuja"
    (with-redefs [oph.heratepalvelu.external.aws-ssm/client mockSsmClient]
      (is (= (ssm/get-secret "random-secret") "asdf"))
      (is (= @saved-results {:name "random-secret" :withDecryption true})))))
