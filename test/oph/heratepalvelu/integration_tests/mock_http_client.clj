(ns oph.heratepalvelu.integration-tests.mock-http-client)

(def results (atom []))

(def url-bindings (atom {}))

(defn clear-results [] (reset! results []))

(defn get-results [] @results)

(defn- create-mock-method [method]
  (fn [url & [options]]
    (swap! results conj {:method method :url url :options options})
    (or (get @url-bindings [method url options])
        (get @url-bindings [method url]))))

(def mock-delete (create-mock-method :delete))
(def mock-get (create-mock-method :get))
(def mock-patch (create-mock-method :patch))
(def mock-post (create-mock-method :post))

(defn bind-url [method url options value]
  (swap! url-bindings assoc [method url options] value))

(defn sloppy-bind-url [method url value]
  (swap! url-bindings assoc [method url] value))

(defn clear-url-bindings [] (reset! url-bindings {}))
