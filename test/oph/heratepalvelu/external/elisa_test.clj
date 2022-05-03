(ns oph.heratepalvelu.external.elisa-test
  (:require [cheshire.core :refer [generate-string]]
            [clojure.test :refer :all]
            [oph.heratepalvelu.external.elisa :as elisa]
            [oph.heratepalvelu.test-util :as tu])
  (:import (clojure.lang ExceptionInfo)))

(use-fixtures :each tu/clear-logs-before-test)

(def mock-tep-msg-body
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
       "effectively. "
       "The survey assesses the institution, not the student."
       "\n\n"
       "kysely.linkki/123\n\n"
       "Testilaitos, Testilaitos 2\n\n"
       "Osoitelähde Opetushallituksen (OPH) eHOKS-rekisteri"))

(deftest test-tep-msg-body
  (testing "Varmista, että msg-body tekee formatointia oikein"
    (is (= (elisa/tep-msg-body "kysely.linkki/123" [{:fi "Testilaitos"}
                                                    {:fi "Testilaitos 2"}])
           mock-tep-msg-body))))

(def mock-tep-muistutus-msg-body
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
       "kysely.linkki/123"
       "\n\n"
       "Kiitos, että vastaat - Tack för att du svarar - Thank you for "
       "responding!"
       "\n\n"
       "Testilaitos, Testilaitos 2"
       "\n\n"
       "Osoitelähde Opetushallituksen (OPH) eHOKS-rekisteri"))

(deftest test-tep-muistutus-msg-body
  (testing "Varmista, että muistutus-msg-body tekee formatointia oikein"
    (is (= (elisa/tep-muistutus-msg-body
             "kysely.linkki/123"
             [{:fi "Testilaitos"} {:fi "Testilaitos 2"}])
           mock-tep-muistutus-msg-body))))

(deftest test-send-sms-send-messages-disabled
  (testing "send-sms palauttaa oikean arvon, kun viestit eivät lähde"
    (with-redefs [clojure.tools.logging/log* tu/mock-log*
                  environ.core/env {:send-messages "false"}]
      (let [number "0401234567"
            message "Test message"
            results {:body {:messages {:0401234567 {:converted "0401234567"
                                                    :status "mock-lahetys"}}}}]
        (is (= (elisa/send-sms number message) results))
        (is (tu/logs-contain? {:level :info :message message}))))))

(defn- mock-client-post [url options] {:url url :options options})

(deftest test-send-sms-no-error
  (testing "Varmista, että send-tep-sms kutsuu client/post oikein"
    (with-redefs [environ.core/env {:send-messages "true"}
                  oph.heratepalvelu.external.elisa/apikey (delay "asdf")
                  oph.heratepalvelu.external.http-client/post mock-client-post]
      (let [number "0401234567"
            message "Test message"
            results {:url "https://viestipalvelu-api.elisa.fi/api/v1/"
                     :options {:headers {:Authorization "apikey asdf"
                                         :content-type "application/json"}
                               :body    (generate-string
                                          {:sender "OPH"
                                           :destination ["0401234567"]
                                           :text "Test message"})
                               :as      :json}}]
        (is (= (elisa/send-sms number message) results))))))

(defn- mock-client-post-with-error [_ _] (throw (ex-info "ABCDE" {})))

(deftest test-send-sms-with-error
  (testing "Varmista, että send-tep-sms käsittelee virheitä oikein"
    (with-redefs [clojure.tools.logging/log* tu/mock-log*
                  environ.core/env {:send-messages "true"}
                  oph.heratepalvelu.external.elisa/apikey (delay "asdf")
                  oph.heratepalvelu.external.http-client/post
                  mock-client-post-with-error]
      (let [number "0401234567"
            message "Test message"
            expected-log-line "Virhe send-tep-sms -funktiossa"]
        (is (thrown? ExceptionInfo (elisa/send-sms number message)))
        (is (tu/logs-contain? {:level :error :message expected-log-line}))
        (is (tu/logs-contain-matching? :error #"(?s).*ABCDE.*"))))))
