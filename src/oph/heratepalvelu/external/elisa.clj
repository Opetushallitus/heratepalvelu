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
       "Kiitos koulutussopimus-/oppisopimusopiskelijoiden ohjaamisesta! "
       "Kerro, miten yhteistyömme onnistui. "
       "Kyselyssä arvioidaan oppilaitosta (ei opiskelijaa). "
       "Palautteella kehitämme toimintaamme."
       "\n\n"
       "Tack för att du handleder studerande på utbildningsavtal/läroavtal! "
       "Berätta gärna hur vårt samarbete fungerade. "
       "I enkäten utvärderas läroanstalten (inte den studerande). "
       "Responsen använder vi för att utveckla vår verksamhet."
       "\n\n"
       "Thank you for guiding students with a training "
       "agreement/apprenticeship! Please, tell us how our co-operation "
       "worked. The survey assesses the institution, not the student."
       "\n\n"
       linkki
       "\n\n"
       "Osoitelähde Opetushallituksen (OPH) eHOKS-rekisteri"))

(defn tep-muistutus-msg-body
  "Luo työpaikkaohjaajakyselyn muistutuksen viestin tekstin."
  [linkki oppilaitokset]
  (str (str/join ", " (map :fi oppilaitokset)) ": "
       "Muistutus: Työpaikkaohjaajakysely - "
       "Påminnelse: Enkät till arbetsplatshandledaren - "
       "Reminder: Survey to workplace instructors"
       "\n\n"
       "Hyvä työpaikkaohjaaja, muistathan antaa palautetta oppilaitokselle. "
       "Kiitos, että vastaat! - "
       "Bästa arbetsplatshandledare, kom ihåg att ge din respons till "
       "läroanstalten. Tack för att du svarar! - "
       "Dear workplace instructor, please give your feedback to the "
       "institution. Thank you for responding!"
       "\n\n"
       linkki
       "\n\n"
       "Osoitelähde Opetushallituksen (OPH) eHOKS-rekisteri"))

(defn amis-msg-body
  "Luo opiskelijapalauteviestin tekstin."
  [linkki oppilaitos]
  (str oppilaitos ": Palautetta oppilaitokselle - "
       "Respons till läroanstalten - Feedback to educational institution."
       "\n\n"
       "Arvostamme näkemyksiäsi. Kerro, missä onnistuimme ja mitä voisimme "
       "tehdä paremmin.  Palaute annetaan nimettömänä."
       "\n\n"
       "Vi uppskattar dina synpunkter. Berätta gärna för oss vad vi gjorde "
       "bra och vad vi skulle kunna göra bättre.  Responsen samlas in anonymt."
       "\n\n"
       "We appreciate your opinions. Please tell us, where we did well, "
       "and what could we do better.  Your feedback is collected anonymously."
       "\n\n"
       linkki
       "\n\n"
       "Osoitelähde Opetushallituksen (OPH) eHOKS-rekisteri"))

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
