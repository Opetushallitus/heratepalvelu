(ns oph.heratepalvelu.external.elisa
  "Apufunktiot SMS-viestien luomiseen ja lähettämiseen."
  (:require [cheshire.core :refer [generate-string]]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.external.aws-ssm :as ssm]
            [oph.heratepalvelu.external.http-client :as client])
  (:import (clojure.lang ExceptionInfo)))

(def ^:private apikey
  "Elisan API key."
  (delay
    (ssm/get-secret
      (str "/" (:stage env) "/services/heratepalvelu/elisa-sms-dialogi-key"))))

(defn tep-msg-body
  "Luo työpaikkaohjaajakyselyn viestin tekstin."
  [linkki oppilaitokset]
  (str (str/join ", " (map :fi oppilaitokset)) ": "
       "Työpaikkaohjaajakysely - Enkät till arbetsplatshandledaren - "
       "Survey to workplace instructors"
       "\n\n"
       linkki
       "?t=s"
       "\n\n"
       "Kiitos koulutussopimus-/oppisopimusopiskelijoiden ohjaamisesta! "
       "Pyydämme arvioimaan oppilaitoksemme toimintaa ja yhteistyömme "
       "onnistumista."
       "\n\n"
       "Tack för att du handleder studerande på utbildningsavtal/läroavtal! "
       "Utvärdera gärna vår läroanstalts verksamhet och hur vårt samarbete "
       "har lyckats."
       "\n\n"
       "Thank you for guiding students with a training "
       "agreement/apprenticeship! Please evaluate the activities of our "
       "institution and the success of our co-operation."
       "\n\n"
       "Osoitelähde: Opetushallitus, eHOKS-rekisteri"))

(defn tep-muistutus-msg-body
  "Luo työpaikkaohjaajakyselyn muistutuksen viestin tekstin."
  [linkki oppilaitokset]
  (str (str/join ", " (map :fi oppilaitokset)) ": "
       "Muistutus: Työpaikkaohjaajakysely - "
       "Påminnelse: Enkät till arbetsplatshandledaren - "
       "Reminder: Survey to workplace instructors"
       "\n\n"
       linkki
       "?t=s"
       "\n\n"
       "Muistathan antaa meille palautetta. Kiitos, kun vastaat!"
       "\n\n"
       "Kom ihåg att ge din respons till läroanstalten. Tack för att du svarar!"
       "\n\n"
       "Please give your feedback to the institution. Thank you for responding!"
       "\n\n"
       "Osoitelähde: Opetushallitus, eHOKS-rekisteri"))

(defn amis-msg-body
  "Luo opiskelijapalauteviestin tekstin."
  [linkki oppilaitos]
  (str oppilaitos ": Päättökysely - Slutenkät - VET-feedback survey"
       "\n\n"
       linkki
       "?t=s"
       "\n\n"
       "Missä onnistuimme ja mitä voisimme tehdä paremmin? "
       "Kiitos, kun vastaat! Palaute annetaan nimettömänä."
       "\n\n"
       "Vad har vi lyckats med och vad kan vi göra bättre? "
       "Tack för att du svarar! Responsen ges anonymt."
       "\n\n"
       "Where did we succeed? What could we do better? "
       "Thank you for replying! All feedback is anonymous."
       "\n\n"
       "Osoitelähde: Opetushallitus, eHOKS-rekisteri"))

(defn send-sms
  "Lähettää SMS-viestin viestintäpalveluun."
  [number message]
  (if (= "true" (:send-messages env))
    (try
      (client/post "https://viestipalvelu-api.elisa.fi/api/v1/"
                   {:headers {:Authorization  (str "apikey " @apikey)
                              :content-type "application/json"}
                    :body    (generate-string {:sender "OPH"
                                               :destination [number]
                                               :text message})
                    :as      :json})
      (catch ExceptionInfo e
        (log/error "Virhe send-sms -funktiossa")
        (log/error e)
        (throw e)))
    (do
      (log/info message)
      {:body
       {:messages {(keyword number) {:converted number
                                     :status "mock-lahetys"}}}})))
