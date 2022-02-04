(ns oph.heratepalvelu.integration-tests.mock-http-client)

(def results (atom []))

(defn clear-results [] (reset! results []))

(defn get-results [] (vec (reverse @results)))

(defn- create-mock-method [method]
  (fn [url & [options]]
    (reset! results
            (cons {:method method :url url :options options} @results))))

(def mock-delete (create-mock-method :delete))
;(def mock-get (create-mock-method :get))
(def mock-patch (create-mock-method :patch))
(def mock-post (create-mock-method :post))

;; Tämä mahdollistaa get-kyselyiden mockaamisen
(def get-url-bindings (atom {}))

(defn bind-get-url [url value] (swap! get-url-bindings assoc url value))

(defn clear-url-bindings [] (reset! get-url-bindings {}))

(def mock-get (let [save-params (create-mock-method :get)]
                (fn [url & [options]]
                  (save-params url options)
                  (get @get-url-bindings url))))
