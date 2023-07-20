(ns oph.heratepalvelu.external.http-client-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.external.http-client :as client]))

(def mock-client-options {:headers {"Caller-Id" "test-caller-id"
                                    "CSRF" "test-caller-id"}
                          :cookies {"CSRF" {:value "test-value" :path "/"}}})

(deftest test-merge-options
  (testing "merge-options käsittelee optioita ja headereitä oikein"
    (with-redefs [oph.heratepalvelu.external.http-client/client-options
                  mock-client-options]
      (let [test-options {:headers {"Test-Header" "test-header-value"}
                          :test-field "test-option"}
            results {:headers {"Caller-Id" "test-caller-id"
                               "CSRF" "test-caller-id"
                               "Test-Header" "test-header-value"}
                     :cookies {"CSRF" {:value "test-value" :path "/"}}
                     :test-field "test-option"}]
        (is (= (client/merge-options test-options) results))))))

(defn- mock-wrap-aws-xray [url method request-func]
  {:url url :method method :request-func-result (request-func)})

(defn mock-http-request [options]
  {:method-name (:method options) :url (:url options)
   :options (dissoc options :method :url)})

(deftest test-method-calls
  (testing "Varmista, että http-client metodit toimivat oikein"
    (with-redefs [clj-http.client/request mock-http-request
                  oph.heratepalvelu.external.aws-xray/wrap-aws-xray
                  mock-wrap-aws-xray
                  oph.heratepalvelu.external.http-client/client-options
                  mock-client-options]
      (let [test-url "https://example.com"
            test-options (assoc mock-client-options :option-field "option-val")
            delete-results {:url test-url
                            :method :delete
                            :request-func-result {:method-name :delete
                                                  :url test-url
                                                  :options test-options}}
            get-results {:url test-url
                         :method :get
                         :request-func-result {:method-name :get
                                               :url test-url
                                               :options test-options}}
            patch-results {:url test-url
                           :method :patch
                           :request-func-result {:method-name :patch
                                                 :url test-url
                                                 :options test-options}}
            post-results {:url test-url
                          :method :post
                          :request-func-result {:method-name :post
                                                :url test-url
                                                :options test-options}}]
        (is (= (client/delete test-url test-options) delete-results))
        (is (= (client/get test-url test-options) get-results))
        (is (= (client/patch test-url test-options) patch-results))
        (is (= (client/post test-url test-options) post-results))))))

(defn mock-http-request-failing-times [times status]
  (let [left (atom times)]
    (fn [options]
      (when (< 0 @left)
        (swap! left dec)
        (throw (ex-info "clj-http error" {:status status})))
      {:status 200 :body "great"})))

(deftest test-retries-temp-errors
  (testing "Väliaikaisista virheistä kokeillaan uudelleen"
    (with-redefs [clj-http.client/request
                  (mock-http-request-failing-times 1 503)
                  oph.heratepalvelu.external.aws-xray/wrap-aws-xray
                  (fn [_ _ f] (f))
                  oph.heratepalvelu.external.http-client/client-options
                  mock-client-options]
      (is (= (client/get "http://foo.bar") {:status 200 :body "great"})))))

(deftest test-retries-too-many-errors
  (testing "Väliaikaisista virheistä kokeillaan uudelleen vain kahdesti"
    (with-redefs [clj-http.client/request
                  (mock-http-request-failing-times 3 503)
                  oph.heratepalvelu.external.aws-xray/wrap-aws-xray
                  (fn [_ _ f] (f))
                  oph.heratepalvelu.external.http-client/client-options
                  mock-client-options]
      (is (thrown? clojure.lang.ExceptionInfo (client/get "http://foo.bar"))))))

(deftest test-retries-permanent-errors
  (testing "Pysyvistä virheistä ei kokeilla uudelleen"
    (with-redefs [clj-http.client/request
                  (mock-http-request-failing-times 1 403)
                  oph.heratepalvelu.external.aws-xray/wrap-aws-xray
                  (fn [_ _ f] (f))
                  oph.heratepalvelu.external.http-client/client-options
                  mock-client-options]
      (is (thrown? clojure.lang.ExceptionInfo (client/get "http://foo.bar"))))))
