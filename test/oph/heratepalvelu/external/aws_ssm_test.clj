(ns oph.heratepalvelu.external.aws-ssm-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.external.aws-ssm :as ssm]))

(definterface IMockGetParameterRequestBuilder
  (build [])
  (name [name-value])
  (withDecryption [decrypt]))

(deftype MockGetParameterRequestBuilder [contents]
  IMockGetParameterRequestBuilder
  (build [this] contents)
  (name [this name-value]
    (MockGetParameterRequestBuilder. (assoc contents :name name-value)))
  (withDecryption [this decrypt]
    (MockGetParameterRequestBuilder. (assoc contents :withDecryption decrypt))))

(definterface IMockParameter
  (value []))

(deftype MockParameter [parameter-value]
  IMockParameter
  (value [this] parameter-value))

(definterface IMockGetParameterResponse
  (parameter []))

(deftype MockGetParameterResponse [value]
  IMockGetParameterResponse
  (parameter [this] (MockParameter. value)))

(def saved-results (atom {}))

(definterface IMockSsmClient
  (getParameter [req]))

(deftype MockSsmClient []
  IMockSsmClient
  (getParameter [this req]
    (reset! saved-results req)
    (MockGetParameterResponse. "asdf")))

(deftest test-get-secret
  (testing "Varmista, että get-secret tekee oikeite kutsuja"
    (with-redefs
      [oph.heratepalvelu.external.aws-ssm/client (MockSsmClient.)
       oph.heratepalvelu.external.aws-ssm/create-get-parameter-request-builder
       (fn [] (MockGetParameterRequestBuilder. {}))]
      (is (= (ssm/get-secret "random-secret") "asdf"))
      (is @saved-results {:name "random-secret" :withDecryption true}))))
