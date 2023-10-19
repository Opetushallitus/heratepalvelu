(ns oph.heratepalvelu.integration-tests.mock-cas-client)

(def results (atom []))

(def url-bindings (atom {}))

(defn clear-results [] (reset! results []))
(defn clear-url-bindings [] (reset! url-bindings {}))

(defn get-results [] (vec (reverse @results)))

(defn bind-url [method url body value]
  (swap! url-bindings assoc [method url body] value))

(defn mock-cas-authenticated-get [url & [options]]
  (reset! results (cons {:method :get :url url :options options} @results))
  (get @url-bindings [:get url nil]))

(defn mock-cas-authenticated-post [url body & [options]]
  (reset! results
          (cons {:method :post :url url :body body :options options} @results))
  (get @url-bindings [:post url body]))

(defn mock-get-service-ticket
  [service suffix]
  (reset! results (cons {:type :get-service-ticket
                         :service service
                         :suffix suffix}
                        @results))
  (str "service-ticket" service "/" suffix))
