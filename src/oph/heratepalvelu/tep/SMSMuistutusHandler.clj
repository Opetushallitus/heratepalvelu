;(ns oph.heratepalvelu.tep.SMSMuistutusHandler
;  (:require [oph.heratepalvelu.db.dynamodb :as ddb]
;
;            [oph.heratepalvelu.external.arvo :refer [get-kyselylinkki-status]]
;            [oph.heratepalvelu.log.caller-log :refer :all]
;            [oph.heratepalvelu.common :as c]
;            [clojure.tools.logging :as log]
;            [clj-time.core :as t])
;  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))
;
;(gen-class
;  :name "oph.heratepalvelu.tep.SMSMuistutusHandler"
;  :methods [[^:static handleSendSMSMuistutus
;             [com.amazonaws.services.lambda.runtime.events.ScheduledEvent
;              com.amazonaws.services.lambda.runtime.Context] void]])
;
;(defn- sendAMISMuistutus [muistutettavat]
;  (log/info (str "Käsitellään " (count muistutettavat) " muistutusta."))
;  (doseq [email muistutettavat]
;    (let [status (get-kyselylinkki-status (:kyselylinkki email))]
;      (if (and (not (:vastattu status))
;               (has-time-to-answer? (:voimassa_loppupvm status)))
;        (try
;          (let [id (:id (send-email {:subject "Muistutus-påminnelse-reminder: Vastaa kyselyyn - svara på enkäten - answer the survey"
;                                     :body (amismuistutus-html email)
;                                     :address (:sahkoposti email)
;                                     :sender "Opetushallitus – Utbildningsstyrelsen"}))]
;            (ddb/update-item
;              {:toimija_oppija [:s (:toimija_oppija email)]
;               :tyyppi_kausi   [:s (:tyyppi_kausi email)]}
;              {:update-expr    (str "SET #muistutukset = :muistutukset, "
;                                    "#vpid = :vpid, "
;                                    "#lahetystila = :lahetystila, "
;                                    "#muistutuspvm = :muistutuspvm")
;               :expr-attr-names {"#muistutukset" "muistutukset"
;                                 "#vpid" "viestintapalvelu-id"
;                                 "#lahetystila" "lahetystila"
;                                 "#muistutuspvm" (str n ".-muistutus-lahetetty")}
;               :expr-attr-vals  {":muistutukset" [:n n]
;                                 ":vpid" [:n id]
;                                 ":lahetystila" [:s (:viestintapalvelussa kasittelytilat)]
;                                 ":muistutuspvm" [:s (str (t/today))]}}))
;          (catch AwsServiceException e
;            (log/error "Muistutus " email " lähetty viestintäpalveluun, muttei päivitetty kantaan!")
;            (log/error e))
;          (catch Exception e
;            (log/error "Virhe muistutuksen lähetyksessä!" email)
;            (log/error e)))
;        (try
;          (ddb/update-item
;            {:toimija_oppija [:s (:toimija_oppija email)]
;             :tyyppi_kausi   [:s (:tyyppi_kausi email)]}
;            {:update-expr     (str "SET #lahetystila = :lahetystila, "
;                                   "#muistutukset = :muistutukset")
;             :expr-attr-names {"#lahetystila" "lahetystila"
;                               "#muistutukset" "muistutukset"}
;             :expr-attr-vals {":lahetystila" [:s (if (:vastattu status)
;                                                   (:vastattu kasittelytilat)
;                                                   (:vastausaika-loppunut-m kasittelytilat))]
;                              ":muistutukset" [:n n]}})
;          (catch Exception e
;            (log/error "Virhe lähetystilan päivityksessä herätteelle, johon on vastattu tai jonka vastausaika umpeutunut" email)
;            (log/error e)))))))
;
;(defn- query-muistukset [n]
;  (ddb/query-items {:muistutukset [:eq [:n (- n 1)]]
;                    :lahetyspvm  [:between
;                                  [[:s (str
;                                         (t/minus
;                                           (t/today)
;                                           (t/days (- (* 5 (+ n 1)) 1))))]
;                                   [:s (str
;                                         (t/minus
;                                           (t/today)
;                                           (t/days (* 5 n))))]]]}
;                   {:index "muistutusIndex"
;                    :limit 50}))
;
;(defn -handleSendAMISMuistutus [this event context]
;  (log-caller-details-scheduled "handleSendAMISMuistutus" event context)
;  (loop [muistutettavat1 (query-muistukset 1)
;         muistutettavat2 (query-muistukset 2)]
;    (sendAMISMuistutus muistutettavat1 1)
;    (sendAMISMuistutus muistutettavat2 2)
;    (when (and
;            (or (seq muistutettavat1) (seq muistutettavat2))
;            (< 60000 (.getRemainingTimeInMillis context)))
;      (recur (query-muistukset 1)
;             (query-muistukset 2)))))
