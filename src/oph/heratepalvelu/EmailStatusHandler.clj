(ns oph.heratepalvelu.EmailStatusHandler)

(gen-class
  :name "oph.heratepalvelu.EmailStatusHandler"
  :methods [[^:static handleEmailStatus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn -handleEmailStatus [this even context])

;(defn get-email-status [id]
;  (:body (cas-authenticated-post
;           (str (:viestintapalvelu-url env) "/status")
;           (str id)
;           {:as :json})))