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
  (str "Työpaikkaohjaajakysely - Enkät till arbetsplatshandledaren - "
       "Survey to workplace instructors."
       "\n\n"
       "Kiitos koulutussopimus-/oppisopimusopiskelijoiden ohjaamisesta! "
       "Pyydämme vastaamaan tähän kyselyyn (5 min) yhteistyömme "
       "kehittämiseksi. "
       "Kyselyssä arvioidaan oppilaitoksen toimintaa, ei opiskelijaa."
       "\n\n"
       "Tack för att du handleder studerande på utbildnings-/läroavtal! "
       "Vi ber dig svara på den här enkäten (5 min) för att utveckla vårt "
       "samarbete. "
       "Responsen utvärderar utbildningsanordnarens verksamhet, inte "
       "studerande."
       "\n\n"
       "Thank you for guiding students with a training "
       "agreement/apprenticeship! "
       "Please respond to this survey (5 min) to help us work together more "
       "effectively. The survey assesses the institution, not the student."
       "\n\n"
       linkki "\n\n"
       (str/join ", " (map :fi oppilaitokset)) "\n\n"
       "Osoitelähde Opetushallituksen (OPH) eHOKS-rekisteri"))

(defn tep-muistutus-msg-body
  "Luo työpaikkaohjaajakyselyn muistutuksen viestin tekstin."
  [linkki oppilaitokset]
  (str "Muistutus-påminnelse-reminder: Työpaikkaohjaajakysely - "
       "Enkät till arbetsplatshandledaren - "
       "Survey to workplace instructors"
       "\n\n"
       "Hyvä työpaikkaohjaaja, olethan muistanut antaa palautetta "
       "oppilaitokselle - "
       "Bästa arbetsplatshandledare, kom ihåg att ge respons till "
       "läroanstalten - "
       "Dear workplace instructor, please remember to give feedback to the "
       "educational institution!"
       "\n\n"
       linkki
       "\n\n"
       "Kiitos, että vastaat - Tack för att du svarar - "
       "Thank you for responding!"
       "\n\n"
       (str/join ", " (map :fi oppilaitokset))
       "\n\n"
       "Osoitelähde Opetushallituksen (OPH) eHOKS-rekisteri"))

(defn amis-msg-body
  "Luo opiskelijapalauteviestin tekstin."
  [linkki oppilaitos]
  (str "Palautetta oppilaitokselle - Respons till läroanstalten - Feedback to educational institution"
       "\n\n"
       "Kokemuksesi koulutuksesta ovat tärkeitä. "
       "Vastaa tähän kyselyyn (10-15 min) ja kerro meille, "
       "missä olemme onnistuneet ja mitä voisimme tehdä vielä paremmin. Palaute annetaan nimettömänä."
       "\n\n"
       "Dina erfarenheter av utbildningen är viktiga för oss. "
       "Vi hoppas att du vill svara på den här enkäten (10–15 min) "
       "och berätta hur vi har lyckats och vad vi kunde göra bättre. "
       "Responsen ges anonymt."
       "\n\n"
       "Your experiences in education and training matter. "
       "Respond to this survey (10-15 min) and tell us what we did well and what we could do even better. "
       "Your feedback is collected anonymously."
       "\n\n"
       linkki
       "\n\n"
       (str/join ", " (map :fi oppilaitos))
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
