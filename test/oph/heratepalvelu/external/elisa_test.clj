(ns oph.heratepalvelu.external.elisa-test
  (:require [cheshire.core :refer [generate-string]]
            [clojure.test :refer :all]
            [oph.heratepalvelu.external.elisa :as elisa]
            [oph.heratepalvelu.test-util :as tu])
  (:import (clojure.lang ExceptionInfo)))

(use-fixtures :each tu/clear-logs-before-test)

(def mock-tep-msg-body
  (str "Testilaitos, Testilaitos 2: Työpaikkaohjaajakysely - "
       "Enkät till arbetsplatshandledaren.\n\nKiitos "
       "koulutussopimus-/oppisopimusopiskelijoiden ohjaamisesta! "
       "Kerro, miten yhteistyömme onnistui. Kyselyssä arvioidaan "
       "oppilaitosta (ei opiskelijaa). Palautteella kehitämme "
       "toimintaamme.\n\nTack för att du handleder studerande på "
       "utbildningsavtal/läroavtal! Berätta gärna hur vårt samarbete "
       "fungerade. I enkäten utvärderas läroanstalten (inte den studerande). "
       "Responsen använder vi för att utveckla vår verksamhet.\n\n"
       "Thank you for guiding students with a training "
       "agreement/apprenticeship! Please, tell us how our co-operation worked. "
       "The survey assesses the institution, not the student.\n\n"
       "kysely.linkki/123\n\nOsoitelähde Opetushallituksen (OPH) "
       "eHOKS-rekisteri"))

(deftest test-tep-msg-body
  (testing "Varmista, että msg-body tekee formatointia oikein"
    (is (= (elisa/tep-msg-body "kysely.linkki/123" [{:fi "Testilaitos"}
                                                    {:fi "Testilaitos 2"}])
           mock-tep-msg-body))))

(def mock-tep-muistutus-msg-body
  (str "Testilaitos, Testilaitos 2: Muistutus: Työpaikkaohjaajakysely - "
       "Påminnelse: Enkät till arbetsplatshandledaren\n\nHyvä "
       "työpaikkaohjaaja, muistathan antaa palautetta oppilaitokselle. "
       "Kiitos, että vastaat! - Bästa arbetsplatshandledare, kom ihåg att "
       "ge din respons till läroanstalten. Tack för att du svarar!"
       " - Dear workplace instructor, please give your feedback to the "
       "institution. Thank you for responding!\n\n"
       "kysely.linkki/123\n\nOsoitelähde Opetushallituksen (OPH) "
       "eHOKS-rekisteri"))

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
      (let [number "12345"
            message "Test message"
            results {:body {:messages {:12345 {:converted "12345"
                                               :status "mock-lahetys"}}}}]
        (is (= (elisa/send-sms number message) results))
        (is (tu/logs-contain? {:level :info :message message}))))))

(defn- mock-client-post [url options] {:url url :options options})

(deftest test-send-sms-no-error
  (testing "Varmista, että send-sms kutsuu client/post oikein"
    (with-redefs [environ.core/env {:send-messages "true"}
                  oph.heratepalvelu.external.elisa/apikey (delay "asdf")
                  oph.heratepalvelu.external.http-client/post mock-client-post]
      (let [number "12345"
            message "Test message"
            results {:url "https://viestipalvelu-api.elisa.fi/api/v1/"
                     :options {:headers {:Authorization "apikey asdf"
                                         :content-type "application/json"}
                               :body    (generate-string
                                          {:sender "OPH"
                                           :destination ["12345"]
                                           :text "Test message"})
                               :as      :json}}]
        (is (= (elisa/send-sms number message) results))))))

(defn- mock-client-post-with-error [_ _] (throw (ex-info "ABCDE" {})))

(deftest test-send-sms-with-error
  (testing "Varmista, että send-sms käsittelee virheitä oikein"
    (with-redefs [clojure.tools.logging/log* tu/mock-log*
                  environ.core/env {:send-messages "true"}
                  oph.heratepalvelu.external.elisa/apikey (delay "asdf")
                  oph.heratepalvelu.external.http-client/post
                  mock-client-post-with-error]
      (let [number "12345"
            message "Test message"
            expected-log-line "Virhe send-sms -funktiossa"]
        (is (thrown? ExceptionInfo (elisa/send-sms number message)))
        (is (tu/logs-contain? {:level :error :message expected-log-line}))
        (is (tu/logs-contain-matching? :error #"(?s).*ABCDE.*"))))))
