(ns oph.heratepalvelu.external.arvo
  (:require [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.external.http-client :as client]
            [cheshire.core :refer [generate-string]]))

(defn build-arvo-request-body [alkupvm
                               request-id
                               kyselytyyppi
                               koulutustoimija
                               oppilaitos
                               tutkintotunnus
                               suorituskieli]
  (assoc {} :vastaamisajan_alkupvm          alkupvm
            :kyselyn_tyyppi                 kyselytyyppi
            :tutkintotunnus                 tutkintotunnus
            :tutkinnon_suorituskieli        suorituskieli
            :koulutustoimija_oid            koulutustoimija
            :oppilaitos_oid                 oppilaitos
            :request_id                     request-id
            :toimipiste_oid                 nil
            :hankintakoulutuksen_toteuttaja nil))<

(defn get-kyselylinkki [data]
  (log/info data)
  (let [resp (client/post (:arvo-url env)
                                 {:content-type "application/json"
                                  :body (generate-string data)
                                  :basic-auth [(:arvo-user env) (:arvo-pwd env)]
                                  :as :json})]
    (log/info resp)
    (if (get-in resp [:body :errors])
      (log/error (:body resp))
      (get-in resp [:body :kysely_linkki]))))

(defn deactivate-kyselylinkki [linkki]
  (log/info "Linkki deaktivoitu"))
