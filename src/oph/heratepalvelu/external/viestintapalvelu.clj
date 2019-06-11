(ns oph.heratepalvelu.external.viestintapalvelu
  (:require [clojure.tools.logging :as log]
            [oph.heratepalvelu.external.http-client :refer [post]]
            [environ.core :refer [env]])
  (:use hiccup.core))

(defn- amispalaute-html [data]
  (str "<!DOCTYPE html>"
       (html [:html {:lang (:suorituskieli data)}
              [:head [:title "amispalaute"]]
              [:body
               [:div
                [:p "Vastaa opiskelijapalautekyselyyn tästä linkistä"]
                [:a {:href (:kyselylinkki data)} "Linkki"]]]])))

(defn send-email [data]
  (log/info "Sending " data)
  (post (:viestintapalvelu-url env)
        {:content-type :json
         :form-params {:recipient [{:email (:email data)}]
                       :email {:from "no-reply@opintopolku.fi"
                               :subject "Ammattikoulu palaute"
                               :isHtml true
                               :body (amispalaute-html data)}}}))