(ns oph.heratepalvelu.integration-tests.mock-http-client)

(def results (atom []))

(def url-bindings (atom {}))

(defn clear-results [] (reset! results []))

(defn get-results [] (vec (reverse @results)))

(defn- create-mock-method [method]
  (fn [url & [options]]
    (reset! results
            (cons {:method method :url url :options options} @results))
    (get @url-bindings [method url options])))

(def mock-delete (create-mock-method :delete))
(def mock-get (create-mock-method :get))
(def mock-patch (create-mock-method :patch))
(def mock-post (create-mock-method :post))

(defn bind-url [method url options value]
  (swap! url-bindings assoc [method url options] value))

(defn clear-url-bindings [] (reset! url-bindings {}))
