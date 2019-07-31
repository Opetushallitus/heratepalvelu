(ns oph.heratepalvelu.external.ehoks
  (:require [oph.heratepalvelu.external.http-client :as client]
            [oph.heratepalvelu.external.cas-client :as cas]
            [environ.core :refer [env]]
            [clojure.tools.logging :as log]))

(defn get-hoks-by-opiskeluoikeus [opiskeluoikeus-oid]
  (:data
    (:body (client/get
             (str (:ehoks-url env) "hoks/opiskeluoikeus/" opiskeluoikeus-oid)
             {:headers {:ticket (cas/get-service-ticket
                                  "/ehoks-virkailija-backend"
                                  "cas-security-check")}
              :as :json}))))