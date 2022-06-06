(ns oph.heratepalvelu.external.koski
  "Wrapperit Kosken REST-rajapinnan ympäri."
  (:require [oph.heratepalvelu.external.http-client :as client]
            [oph.heratepalvelu.external.aws-ssm :as ssm]
            [environ.core :refer [env]])
  (:import (clojure.lang ExceptionInfo)))

(def ^:private pwd
  "Kosken autentikoinnin salasana."
  (delay (ssm/get-secret
           (str "/" (:stage env) "/services/heratepalvelu/koski-pwd"))))

(defn- koski-get
  "Tekee GET-kyselyn koskeen."
  [uri-path options]
  (client/get (str (:koski-url env) uri-path)
              (merge {:basic-auth [(:koski-user env) @pwd]} options)))

(defn get-opiskeluoikeus
  "Hakee opiskeluoikeuden OID:n perusteella."
  [oid]
  (:body (koski-get (str "/opiskeluoikeus/" oid) {:as :json})))

(defn get-opiskeluoikeus-catch-404
  "Hakee opiskeluoikeuden OID:n perusteella, ja palauttaa nil, jos ilmenee
  404-virhe."
  [oid]
  (try (get-opiskeluoikeus oid)
       (catch ExceptionInfo e
         (when-not (and (:status (ex-data e))
                        (= 404 (:status (ex-data e))))
           (throw e)))))

(defn get-completed-opiskeluoikeudet
  "Hakee opiskeluoikeudet, joihin on tehty päivityksiä datetime-str:n jälkeen."
  [start end page]
  (let [params {"opiskeluoikeudenTyyppi"              "ammatillinenkoulutus"
                "opiskeluoikeusPäättynytAikaisintaan" start
                "opiskeluoikeusPäättynytViimeistään"  end
                "pageSize"                            100
                "pageNumber"                          page}
        resp (koski-get "/oppija/" {:query-params params
                                    :as :json-strict})]
    (sort-by :aikaleima
             (reduce #(into %1 (:opiskeluoikeudet %2)) [] (:body resp)))))
