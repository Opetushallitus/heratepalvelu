(ns oph.heratepalvelu.amis.resendBetweenHandler
  (:require [cheshire.core :refer [parse-string]]
            [oph.heratepalvelu.external.http-client :as client]
            [environ.core :refer [env]]
            [oph.heratepalvelu.external.cas-client :as cas]
            [clojure.tools.logging :as log]))

(gen-class
  :name "oph.heratepalvelu.amis.resendBetweenHandler"
  :methods [[^:static handleResendBetween
             [com.amazonaws.services.lambda.runtime.events.SQSEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn -handleResendBetween [this event context]
  (let [messages (seq (.getRecords event))]
    (doseq [msg messages]
      (let [herate (parse-string (.getBody msg) true)
            from (:from herate)
            to (:to herate)]
        (log/info
          (:body (client/post
                   (str (:ehoks-url env)
                        (str "heratepalvelu/hoks/resend-aloitusherate?from="
                             from "&to=" to))
                   {:headers      {:ticket (cas/get-service-ticket
                                             "/ehoks-virkailija-backend"
                                             "cas-security-check")}
                    :content-type "application/json"
                    :as           :json})))))))
