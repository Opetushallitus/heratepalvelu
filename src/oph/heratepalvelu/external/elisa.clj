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
       "Pyydämme vastaamaan tähän kyselyyn (5 min) yhteistyömme kehittämiseksi."
       "\n\n"
       "Tack för att du handleder studerande på utbildnings-/läroavtal! "
       "Vi ber dig svara på den här enkäten (5 min) för att utveckla vårt samarbete."
       "\n\n"
       "Thank you for guiding students with training agreement/apprenticeship! "
       "Please answer this survey (5 min) to develop our cooperation."
       "\n\n"
       linkki "\n\n"
       (str/join ", " (map :fi oppilaitokset)) "\n\n"
       "Osoitelähde Opetushallituksen (OPH) eHOKS-rekisteri"))

(defn muistutus-msg-body [linkki oppilaitokset]
  (str "Hyvä työpaikkaohjaaja, olethan muistanut antaa palautetta oppilaitokselle"
       "\n\n"
       "Bästa arbetsplatshandledare, kom ihåg att ge respons till läroanstalten"
       "\n\n"
       "Dear workplace instructor, please remember to give feedback to educational institution!"
       "\n\n"
       linkki "\n\n"
       "Kiitos, että vastaat - Tack för att du svarar – Thank you for responding!"
       "\n\n"
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
