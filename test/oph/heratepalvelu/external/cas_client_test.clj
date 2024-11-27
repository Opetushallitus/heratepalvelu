(ns oph.heratepalvelu.external.cas-client-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.external.cas-client :as cas])
  (:import (fi.vm.sade.utils.cas CasClient CasParams)
           (com.amazonaws.xray AWSXRay)))

(deftest test-init-client
  (testing "Varmista, että init-client toimii oikein"
    (with-redefs [environ.core/env {:caller-id "asdf"
                                    :cas-user "user"
                                    :cas-url "example.com"}
                  oph.heratepalvelu.external.cas-client/pwd (delay "p@ssw0rd")]
      (let [expected-client (str "cas-client-placeholder "
                                 "/ryhmasahkoposti-service user p@w0rd")
            expected-params "cas-params-placeholder example.com asdf"
            returned (cas/init-client)]
        (is (:client returned) expected-client)
        (is (:params returned) expected-params)
        (is (nil? @(:session-id returned)))))))

(deftest test-request-with-json-body
  (testing "Varmista, että request-with-json-body toimii oikein"
    (is (= (cas/request-with-json-body {} {:field "value"})
           {:headers {"Content-Type" "application/json"}
            :body "{\"field\":\"value\"}"}))))

(deftest test-create-params
  (testing "Varmista, että create-params luo requestejä oikein"
    (with-redefs [environ.core/env {:caller-id "asdf"}]
      (let [cas-session-id (atom "session-id")
            body {:field "value"}
            results-body {:headers {"Caller-Id"           "asdf"
                                    "clientSubSystemCode" "asdf"
                                    "CSRF"                "asdf"
                                    "Content-Type"        "application/json"}
                          :cookies {"CSRF"       {:value "asdf"
                                                  :path  "/"}
                                    "JSESSIONID" {:value "session-id"
                                                  :path  "/"}}
                          :body    "{\"field\":\"value\"}"
                          :redirect-strategy :none}
            results-no-body {:headers {"Caller-Id"           "asdf"
                                       "clientSubSystemCode" "asdf"
                                       "CSRF"                "asdf"}
                             :cookies {"CSRF"       {:value "asdf"
                                                     :path  "/"}
                                       "JSESSIONID" {:value "session-id"
                                                     :path  "/"}}
                             :redirect-strategy :none}]
        (is (= (cas/create-params cas-session-id body) results-body))
        (is (= (cas/create-params cas-session-id nil) results-no-body))))))

(def mockCasClient
  (proxy [CasClient] ["" nil "abcdef"]
    (fetchCasSession [cas-params session-id-key]
      (scalaz.concurrent.Task/now (str cas-params " " session-id-key)))))

(defn- mock-init-client []
  (cas/map->CasClientWrapper {:client mockCasClient
                              :params (proxy [CasParams] [nil nil]
                                        (toString [] "cas-params-placeholder"))
                              :session-id (atom nil)}))

(def mock-clj-http-client-request-count (atom 0))

(defn- mock-clj-http-client-request [params]
  (swap! mock-clj-http-client-request-count inc)
  {:status (if (.contains ^String (:url params) "302") 302 200) :params params})

(deftest test-cas-http
  (testing "Varmista, että cas-http toimii oikein"
    (with-redefs [clj-http.client/request mock-clj-http-client-request
                  environ.core/env {:caller-id "asdf"}
                  oph.heratepalvelu.external.cas-client/init-client
                  mock-init-client]
      (AWSXRay/beginSegment "test-cas-http")
      (let [url-1 "example.com"
            url-2 "example.com/returns/302"
            options {:option "value"}
            results-1 {:status 200
                       :params
                       {:url "example.com"
                        :method :get
                        :option "value"
                        :headers {"Caller-Id"           "asdf"
                                  "clientSubSystemCode" "asdf"
                                  "CSRF"                "asdf"}
                        :cookies {"CSRF" {:value "asdf" :path "/"}
                                  "JSESSIONID"
                                  {:value "cas-params-placeholder JSESSIONID"
                                   :path  "/"}}
                        :redirect-strategy :none}}
            results-2 {:status 302
                       :params
                       {:url "example.com/returns/302"
                        :method :get
                        :option "value"
                        :headers {"Caller-Id"           "asdf"
                                  "clientSubSystemCode" "asdf"
                                  "CSRF"                "asdf"}
                        :cookies {"CSRF" {:value "asdf" :path "/"}
                                  "JSESSIONID"
                                  {:value "cas-params-placeholder JSESSIONID"
                                   :path  "/"}}
                        :redirect-strategy :none}}
            results-3 {:status 200
                       :params
                       {:url "example.com"
                        :method :post
                        :option "value"
                        :headers {"Caller-Id"           "asdf"
                                  "clientSubSystemCode" "asdf"
                                  "CSRF"                "asdf"
                                  "Content-Type"        "application/json"}
                        :cookies {"CSRF" {:value "asdf" :path "/"}
                                  "JSESSIONID"
                                  {:value "cas-params-placeholder JSESSIONID"
                                   :path  "/"}}
                        :body    "{\"foo\":\"bar\"}"
                        :redirect-strategy :none}}]
        (is (= (cas/cas-authenticated-get url-1 options) results-1))
        (is (= @mock-clj-http-client-request-count 1))
        (is (= (cas/cas-authenticated-post url-1 {:foo "bar"} options)
               results-3))
        (is (= @mock-clj-http-client-request-count 2))
        (reset! mock-clj-http-client-request-count 0)
        (is (= (cas/cas-authenticated-get url-2 options) results-2))
        (is (= @mock-clj-http-client-request-count 2)))
      (AWSXRay/endSegment))))
