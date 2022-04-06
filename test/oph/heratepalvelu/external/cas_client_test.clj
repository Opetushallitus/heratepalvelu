(ns oph.heratepalvelu.external.cas-client-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.external.cas-client :as cas])
  (:import (fi.vm.sade.utils.cas CasClient CasParams)))

(defn- mock-cas-client [url caller-id]
  (str "cas-client-placeholder " url " " caller-id))

(defn- mock-cas-params [path username password]
  (str "cas-params-placeholder " path " " username " " password))

(deftest test-init-client
  (testing "Varmista, että init-client toimii oikein"
    (with-redefs [clj-cas.cas/cas-client mock-cas-client
                  clj-cas.cas/cas-params mock-cas-params
                  environ.core/env {:caller-id "asdf"
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
      (let [method :get
            url-1 "example.com"
            url-2 "example.com/returns/302"
            options {:option "value"}
            body {}
            results-1 {:status 200
                       :params
                       {:url "example.com"
                        :method :get
                        :option "value"
                        :headers {"Caller-Id"           "asdf"
                                  "clientSubSystemCode" "asdf"
                                  "CSRF"                "asdf"
                                  "Content-Type"        "application/json"}
                        :cookies {"CSRF" {:value "asdf" :path "/"}
                                  "JSESSIONID"
                                  {:value "cas-params-placeholder JSESSIONID"
                                   :path  "/"}}
                        :body    "{}"
                        :redirect-strategy :none}}
            results-2 {:status 302
                       :params
                       {:url "example.com/returns/302"
                        :method :get
                        :option "value"
                        :headers {"Caller-Id"           "asdf"
                                  "clientSubSystemCode" "asdf"
                                  "CSRF"                "asdf"
                                  "Content-Type"        "application/json"}
                        :cookies {"CSRF" {:value "asdf" :path "/"}
                                  "JSESSIONID"
                                  {:value "cas-params-placeholder JSESSIONID"
                                   :path  "/"}}
                        :body    "{}"
                        :redirect-strategy :none}}]
        (is (= (cas/cas-http method url-1 options body) results-1))
        (is (= @mock-clj-http-client-request-count 1))
        (reset! mock-clj-http-client-request-count 0)
        (is (= (cas/cas-http method url-2 options body) results-2))
        (is (= @mock-clj-http-client-request-count 2))))))

(defn- mock-wrap-aws-xray [url method request]
  {:url url :method method :request-result (request)})

(defn- mock-cas-http [method url options body]
  {:method method :url url :options options :body body})

(deftest test-cas-authenticated-get-and-post
  (testing "cas-authenticated-get ja cas-authenticated-post toimivat oikein"
    (with-redefs [oph.heratepalvelu.external.aws-xray/wrap-aws-xray
                  mock-wrap-aws-xray
                  oph.heratepalvelu.external.cas-client/cas-http mock-cas-http]
      (let [url "example.com"
            options {:option "value"}
            body {:a "b"}
            results-get {:url "example.com"
                         :method :get
                         :request-result {:method :get
                                          :url "example.com"
                                          :options {:option "value"}
                                          :body nil}}
            results-post {:url "example.com"
                          :method :post
                          :request-result {:method :post
                                           :url "example.com"
                                           :options {:option "value"}
                                           :body {:a "b"}}}]
        (is (= (cas/cas-authenticated-get url options) results-get))
        (is (= (cas/cas-authenticated-post url body options) results-post))))))
