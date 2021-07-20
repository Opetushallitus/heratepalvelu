(ns oph.heratepalvelu.external.elisa
  (:require [oph.heratepalvelu.external.aws-ssm :as ssm]
            [oph.heratepalvelu.external.http-client :as client]
            [cheshire.core :refer [generate-string]]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [clojure.string :as str]))

(def ^:private apikey (delay
                        (ssm/get-secret
                          (str "/" (:stage env)
                               "/services/heratepalvelu/elisa-sms-dialogi-key"))))


(defn msg-body [linkki oppilaitokset]
  (str "Kiitos koulutussopimus-/oppisopimusopiskelijoiden ohjaamisesta! "
       "Pyydämme vastaamaan tähän kyselyyn (5 min) yhteistyömme kehittämiseksi. "
       "Jos olet vastannut kyselyyn sähköpostilla saamasi linkin kautta, palautteesi on jo otettu vastaan, kiitos.\n\n"
       "Tack för att du handleder studerande på utbildnings-/läroavtal! "
       "Vi ber dig svara på den här enkäten (5 min) för att utveckla vårt samarbete. "
       "Om du har svarat på enkäten via länken från e-postmeddelnadet, har din feedback redan tagits emot, tack.\n\n"
       "Thank you for guiding students with training agreement/apprenticeship! "
       "Please answer this survey (5 min) to develop our cooperation. "
       "If you have responded to the survey via the link you received in the email, your feedback has already been received, thank you.\n\n"
       linkki "\n\n"
       (str/join ", " (map :fi oppilaitokset)) "\n\n"
       "Osoitelähde Opetushallituksen (OPH) eHOKS-rekisteri"))

(defn send-tep-sms [number message]
  (if (= "true" (:send-messages env))
    (let [body {:sender "OPH"
                :destination [number]
                :text message}
          resp (client/post
                 (str "https://viestipalvelu-api.elisa.fi/api/v1/")
                 {:headers {:Authorization  (str "apikey " @apikey)
                            :content-type "application/json"}
                  :body        (generate-string body)
                  :as :json})]
      resp)
    (do
      (log/info message)
      {:body
       {:messages {(keyword number) {:converted number
                                     :status "mock-lahetys"}}}})))
