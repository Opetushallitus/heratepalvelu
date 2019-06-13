(ns oph.heratepalvelu.external.viestintapalvelu
  (:require [clojure.tools.logging :as log]
            [cheshire.core :refer [generate-string]]
            [oph.heratepalvelu.external.cas-client :refer [cas-authenticated-post]]
            [environ.core :refer [env]]
            [clj-time.core :refer [now]])
  (:use hiccup.core))

(defn- amispalaute-html [data]
  (str "<!DOCTYPE html>"
       (html [:html {:lang (:suorituskieli data)}
              [:head [:title "amispalaute"]]
              [:body
               [:div
                [:p "Vastaa opiskelijapalautekyselyyn tästä linkistä"]
                [:a {:href (:kyselylinkki data)} "Linkki"]
                [:p (.toString (now))]]]])))

(defn send-email [data]
  (let [resp (cas-authenticated-post
               (:viestintapalvelu-url env)
               {:recipient [{:email (:email data)}]
                :email {:from "no-reply@opintopolku.fi"
                        :subject "Ammattikoulu palaute"
                        :isHtml true
                        :body (amispalaute-html data)}}
               {:as :json})]
    (:body resp)))
