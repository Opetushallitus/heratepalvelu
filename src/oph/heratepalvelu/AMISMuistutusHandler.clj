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

(defn -handleSendAMISMuistutus [this event context]
  (log-caller-details "handleSendAMISMuistutus" event context)
  (loop [muistutettavat (ddb/query-items {:lahetystila [:eq [:s "viestintapalvelussa"]]
                                          :alkupvm     [:eq [:s (.toString (t/minus (t/today) (t/weeks 2)))]]}
                                         {:index "lahetysIndex"
                                          :limit 100})]
    (log/info "Käsitellään " (count muistutettavat) " lähetettävää muistutusta.")
    (when (seq muistutettavat)
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
                  {:update-expr    (str "SET #lahetystila = :lahetystila, "
                                        "#vpid = :vpid")
                   :expr-attr-names {"#lahetystila" "lahetystila"
                                     "#vpid" "viestintapalvelu-id"}
                   :expr-attr-vals  {":lahetystila" [:s "muistutus_viestintapalvelussa"]
                                     ":vpid" [:n id]}}))
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
                 :expr-attr-vals {":lahetystila" [:s "vastattu_tai_vastausaika_loppunut_ennen_muistutusta"]}})
              (catch Exception e
                (log/error "Virhe lähetystilan päivityksessä herätteelle, johon on vastattu tai jonka vastausaika umpeutunut" email)
                (log/error e))))))
      (when (< 30000 (.getRemainingTimeInMillis context))
        (recur (ddb/query-items {:lahetystila [:eq [:s "viestintapalvelussa"]]
                                 :alkupvm     [:eq [:s (.toString (t/minus (t/today) (t/weeks 3)))]]}
                                {:index "lahetysIndex"
                                 :limit 100}))))))
