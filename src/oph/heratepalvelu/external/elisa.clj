(ns oph.heratepalvelu.external.elisa
  (:require [cheshire.core :refer [generate-string]]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [environ.core :refer [env]]
            [oph.heratepalvelu.external.aws-ssm :as ssm]
            [oph.heratepalvelu.external.http-client :as client])
  (:import (clojure.lang ExceptionInfo)))

(def ^:private apikey
  (delay (ssm/get-secret
           (str "/"
                (:stage env)
                "/services/heratepalvelu/elisa-sms-dialogi-key"))))

(defn msg-body
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

(defn muistutus-msg-body
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

(defn send-tep-sms
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
        (log/error "Virhe send-tep-sms -funktiossa")
        (log/error e)
        (throw e)))
    (do
      (log/info message)
      {:body
       {:messages {(keyword number) {:converted number
                                     :status "mock-lahetys"}}}})))
