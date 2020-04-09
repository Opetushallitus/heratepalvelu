(ns oph.heratepalvelu.AMISMuistutusHandler
  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
            [oph.heratepalvelu.external.viestintapalvelu :refer [send-email amismuistutus-html]]
            [oph.heratepalvelu.external.arvo :refer [get-kyselylinkki-status]]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.common :refer [has-time-to-answer?]]
            [clojure.tools.logging :as log]
            [clj-time.core :as t]
            [cheshire.core :refer [parse-string]])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))

(gen-class
  :name "oph.heratepalvelu.AMISMuistutusHandler"
  :methods [[^:static handleSendAMISMuistutus
             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn- sendAMISMuistutus [muistutettavat n]
  (log/info (str "Käsitellään " (count muistutettavat)
                 " lähetettävää " n ". muistutusta."))
  (doseq [email muistutettavat]
    (let [status (get-kyselylinkki-status (:kyselylinkki email))]
      (if (and (not (:vastattu status))
               (has-time-to-answer? (:voimassa_loppupvm status)))
        (try
          (let [id (:id (send-email {:subject "Palaute muistutus"
                                     :body (amismuistutus-html email)
                                     :address (:sahkoposti email)}))]
            (ddb/update-item
              {:toimija_oppija [:s (:toimija_oppija email)]
               :tyyppi_kausi   [:s (:tyyppi_kausi email)]}
              {:update-expr    (str "SET #muistutukset = :muistutukset, "
                                    "#vpid = :vpid, "
                                    "#tarkistettu = :tarkistettu, "
                                    "#muistutuspvm = :muistutuspvm")
               :expr-attr-names {"#muistutukset" "muistutukset"
                                 "#vpid" "viestintapalvelu-id"
                                 "#tarkistettu" "tarkistettu"
                                 "#muistutuspvm" (str "muistutus" n "-pvm")}
               :expr-attr-vals  {":muistutukset" [:n n]
                                 ":vpid" [:n id]
                                 ":tarkistettu" [:bool false]
                                 ":muistutuspvm" [:s (str (t/today))]}}))
          (catch AwsServiceException e
            (log/error "Muistutus " email " lähetty viestintäpalveluun, muttei päivitetty kantaan!")
            (log/error e))
          (catch Exception e
            (log/error "Virhe muistutuksen lähetyksessä!" email)
            (log/error e)))
        (try
          (ddb/update-item
            {:toimija_oppija [:s (:toimija_oppija email)]
             :tyyppi_kausi   [:s (:tyyppi_kausi email)]}
            {:update-expr     "SET #lahetystila = :lahetystila"
             :expr-attr-names {"#lahetystila" "lahetystila"}
             :expr-attr-vals {":lahetystila" [:s "vastattu_tai_vastausaika_loppunut"]}})
          (catch Exception e
            (log/error "Virhe lähetystilan päivityksessä herätteelle, johon on vastattu tai jonka vastausaika umpeutunut" email)
            (log/error e)))))))

(defn- query-muistukset [n]
  (ddb/query-items {:muistutukset [:eq [:n (- n 1)]]
                    :lahetyspvm  [:le
                                  [:s (.toString
                                        (t/minus
                                          (t/today)
                                          (t/days (* 5 n))))]]}
                   {:index "muistutusIndex"
                    :limit 100}))

(defn -handleSendAMISMuistutus [this event context]
  (log-caller-details "handleSendAMISMuistutus" event context)
  (loop [muistutettavat1 (query-muistukset 1)
         muistutettavat2 (query-muistukset 2)]
    (sendAMISMuistutus muistutettavat1 1)
    (sendAMISMuistutus muistutettavat2 2)
    (when (and
            (or (seq muistutettavat1) (seq muistutettavat2))
            (< 30000 (.getRemainingTimeInMillis context)))
      (recur (query-muistukset 1)
             (query-muistukset 2)))))
