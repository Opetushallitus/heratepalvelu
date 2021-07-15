(ns oph.heratepalvelu.external.elisa
  (:require [oph.heratepalvelu.external.aws-ssm :as ssm]
            [oph.heratepalvelu.external.http-client :as client]
            [cheshire.core :refer [generate-string]]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]])
  (:import (com.google.i18n.phonenumbers PhoneNumberUtil)
           (com.google.i18n.phonenumbers NumberParseException)))

(def ^:private apikey (delay
                        (ssm/get-secret
                          (str "/" (:stage env)
                               "/services/heratepalvelu/elisa-sms-dialogi-key"))))

(defn- valid-number? [number]
  (let [utilobj (PhoneNumberUtil/getInstance)
        numberobj (.parse utilobj number "FI")]
    (and (empty? (filter (fn [x] (Character/isLetter x)) number))
         (.isValidNumber utilobj numberobj)
         (= (str (.getNumberType utilobj numberobj))
            "MOBILE"))))

(defn msg-body [linkki oppilaitokset muistutus]
  )

(defn send-tep-sms [number message]
  (try
    (when (and (valid-number? number)
               (= "sade" (:stage env)))
      (let [body {:sender "OPH"
                  :destination [number]
                  :text message}
            resp (client/post
                   (str "https://viestipalvelu-api.elisa.fi/api/v1/")
                   {:headers {:Authorization  (str "apikey " @apikey)
                              :content-type "application/json"}
                    :body        (generate-string body)})]
        resp))
    (catch NumberParseException e
      (log/error "PhoneNumberUtils failed to parse phonenumber " number)
      (throw e))))
