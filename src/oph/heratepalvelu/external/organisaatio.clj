(ns oph.heratepalvelu.external.organisaatio
  "Wrapperit organisaatiopalvelun REST-rajapinnan ympäri."
  (:require [oph.heratepalvelu.external.http-client :as client]
            [environ.core :refer [env]]
            [clojure.tools.logging :as log])
  (:import (clojure.lang ExceptionInfo)))

(defn get-organisaatio
  "Hakee organisaation OID:n perusteella."
  [oid]
  (try
    (:body (client/get (str (:organisaatio-url env) oid) {:as :json}))
    (catch ExceptionInfo e
      (do (log/error "Virhe hausta organisaatiopalvelusta:" e)
          (throw e)))))
