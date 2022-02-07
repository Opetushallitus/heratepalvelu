(ns oph.heratepalvelu.integration-tests.mock-http-client)

(def results (atom []))

(defn clear-results [] (reset! results []))

(defn get-results [] (vec (reverse @results)))

(defn- create-mock-method [method]
  (fn [url & [options]]
    (reset! results
            (cons {:method method :url url :options options} @results))))

(def mock-delete (create-mock-method :delete))
(def mock-patch (create-mock-method :patch))

;; Tämä mahdollistaa get-kyselyiden mockaamisen
(def get-url-bindings (atom {}))
(def post-url-bindings (atom {}))

(defn bind-get-url [url value] (swap! get-url-bindings assoc url value))
(defn bind-post-url [url options value]
  (swap! post-url-bindings assoc {:url url :options options} value))

(defn clear-url-bindings []
  (reset! get-url-bindings {})
  (reset! post-url-bindings {}))

(def mock-get (let [save-params (create-mock-method :get)]
                (fn [url & [options]]
                  (save-params url options)
                  (get @get-url-bindings url))))

(def mock-post (let [save-params (create-mock-method :post)]
                 (fn [url & [options]]
                   (save-params url options)
                   (get @post-url-bindings {:url url :options options}))))
