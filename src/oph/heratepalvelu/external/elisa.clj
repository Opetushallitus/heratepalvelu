(ns oph.heratepalvelu.external.elisa
  (:require [oph.heratepalvelu.external.aws-ssm :as ssm]
            [oph.heratepalvelu.external.http-client :as client]
            [cheshire.core :refer [generate-string]]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [clojure.string :as str])
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

(defn msg-body [linkki oppilaitokset]
  (str "Kiitos koulutussopimus-/oppisopimusopiskelijoiden ohjaamisesta! "
       "Pyydämme vastaamaan tähän kyselyyn (5 min) yhteistyömme kehittämiseksi. "
       "– Tack för att Ni handleder studerande på utbildnings-/läroavtal! "
       "Vi ber er svara på den här enkäten (5 min) för att utveckla vårt samarbete. "
       "– Thank you for guiding students with training agreement/apprenticeship! "
       "Please answer this survey (5 min) to develop our cooperation.\n"
       linkki "\n"
       (str/join ", " (map :fi oppilaitokset)) "\n"
       "Osoitelähde Opetushallituksen (OPH) eHOKS-rekisteri"))

(defn send-tep-sms [number message]
  (try
    (if (= "true" (:send-messages env))
      (when (valid-number? number)
        (let [body {:sender "OPH - UBS"
                    :destination [number]
                    :text message}
              resp (client/post
                     (str "https://viestipalvelu-api.elisa.fi/api/v1/")
                     {:headers {:Authorization  (str "apikey " @apikey)
                                :content-type "application/json"}
                      :body        (generate-string body)
                      :as :json})]
          resp))
      (log/info message))
    (catch NumberParseException e
      (log/error "PhoneNumberUtils failed to parse phonenumber " number)
      (throw e))))
