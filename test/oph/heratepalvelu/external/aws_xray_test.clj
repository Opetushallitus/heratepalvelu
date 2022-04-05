(ns oph.heratepalvelu.external.aws-xray-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.external.aws-xray :as xray])
  (:import (clojure.lang ExceptionInfo)
           (com.amazonaws.xray.entities Segment)))

(def saved-results (atom []))

(def mockSegment
  (proxy [Segment] []
    (putHttp [segment-type options]
      (reset! saved-results (cons {:type "putHttp"
                                   :segment-type segment-type
                                   :options options}
                                  @saved-results)))
    (addException [exception]
      (reset! saved-results (cons {:type "addException"
                                   :exception (ex-data exception)}
                                  @saved-results)))))

(defn- mock-wrap-begin-subsegment [line]
  (reset! saved-results
          (cons {:type "mock-wrap-begin-subsegment" :line line} @saved-results))
  mockSegment)

(defn- mock-wrap-end-subsegment []
  (reset! saved-results
          (cons {:type "mock-wrap-end-subsegment"} @saved-results)))

(defn- mock-request [] {:status 200 :headers {"Content-Length" 10}})

(defn- mock-request-with-error [] (throw (ex-info "Random" {:type "random"})))

(deftest test-wrap-aws-xray
  (testing "Varmista, että wrap-aws-xray tekee oikeita kutsuja"
    (with-redefs [oph.heratepalvelu.external.aws-xray/wrap-begin-subsegment
                  mock-wrap-begin-subsegment
                  oph.heratepalvelu.external.aws-xray/wrap-end-subsegment
                  mock-wrap-end-subsegment]
      (is (= (xray/wrap-aws-xray "example.com" :get mock-request)
             {:status 200 :headers {"Content-Length" 10}}))
      (is (= (vec (reverse @saved-results))
             [{:type "mock-wrap-begin-subsegment" :line "HTTP GET"}
              {:type "putHttp"
               :segment-type "request"
               :options {"method" "GET"
                         "url" "example.com"}}
              {:type "putHttp"
               :segment-type "response"
               :options {"status" 200
                         "content_length" 10}}
              {:type "mock-wrap-end-subsegment"}]))
      (reset! saved-results [])
      (is (thrown? ExceptionInfo (xray/wrap-aws-xray "example.com"
                                                     :get
                                                     mock-request-with-error)))
      (is (= (vec (reverse @saved-results))
             [{:type "mock-wrap-begin-subsegment" :line "HTTP GET"}
              {:type "putHttp"
               :segment-type "request"
               :options {"method" "GET"
                         "url" "example.com"}}
              {:type "addException" :exception {:type "random"}}
              {:type "mock-wrap-end-subsegment"}])))))
