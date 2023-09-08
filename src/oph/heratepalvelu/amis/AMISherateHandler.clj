(ns oph.heratepalvelu.amis.AMISherateHandler
  "Käsittelee SQS:stä saatuja AMIS-herätteitä."
  (:require [cheshire.core :refer [parse-string]]
            [clojure.tools.logging :as log]
            [oph.heratepalvelu.amis.AMISCommon :as ac]
            [oph.heratepalvelu.common :refer :all]
            [oph.heratepalvelu.external.koski
             :refer
             [get-opiskeluoikeus-catch-404!]]
            [oph.heratepalvelu.log.caller-log :refer :all])
  (:import (com.amazonaws.services.lambda.runtime.events SQSEvent$SQSMessage)
           (com.fasterxml.jackson.core JsonParseException)
           (clojure.lang ExceptionInfo)))

(gen-class
  :name "oph.heratepalvelu.amis.AMISherateHandler"
  :methods [[^:static handleAMISherate
             [com.amazonaws.services.lambda.runtime.events.SQSEvent
              com.amazonaws.services.lambda.runtime.Context] void]])

(defn handle-single-herate!
  "Käsittelee yhden SQS-viestin."
  [^SQSEvent$SQSMessage msg]
  (try
    (let [herate (parse-string (.getBody msg) true)
          opiskeluoikeus (get-opiskeluoikeus-catch-404!
                           (:opiskeluoikeus-oid herate))
          koulutustoimija (and opiskeluoikeus
                               (get-koulutustoimija-oid opiskeluoikeus))
          summary (str "opiskeluoikeus-oid: " (:opiskeluoikeus-oid herate)
                       " koulutustoimija: " (or koulutustoimija "ei mikään")
                       " oppija-oid: " (:oppija-oid herate)
                       " kyselytyyppi: " (:kyselytyyppi herate))]
      (log/info "Käsitellään SQS-heräte:" herate)
      (cond
        (not (valid-herate-date? (:alkupvm herate)))
        (log/warn "Ei tallenneta, alkupvm" (:alkupvm herate)
                  "virheellinen:" summary)

        (not (some? opiskeluoikeus))
        (log/error "Ei löytynyt opiskeluoikeutta:" summary)

        (not (has-one-or-more-ammatillisen-tutkinnon-suoritus? opiskeluoikeus))
        (log/warn "Ei tallenneta, opiskeluoikeudessa ei yhtään ammatillisen "
                  "tutkinnon suoritusta:" summary)

        (not (whitelisted-organisaatio?!
               koulutustoimija (date-string-to-timestamp (:alkupvm herate))))
        (log/info "Ei tallenneta, koulutustoimija ei mukana:" summary)

        (sisaltyy-toiseen-opiskeluoikeuteen? opiskeluoikeus)
        (log/info "Ei tallenneta, opiskeluoikeus sisältyy toiseen oikeuteen"
                  (:sisältyyOpiskeluoikeuteen opiskeluoikeus) ":" summary)

        :else (ac/check-and-save-herate! herate opiskeluoikeus koulutustoimija
                                         (:ehoks herate-sources))))
    (catch JsonParseException e
      (log/error e "Virhe viestin lukemisessa:" (.getBody msg) "\n" e))
    (catch ExceptionInfo e
      (log/error e "Käsittelemätön poikkeus viestille:" (.getBody msg)))))

(defn -handleAMISherate
  "Käsittelee herätteitä ja tallentaa ne tietokantaan, jos ne ovat valideja."
  [_ ^com.amazonaws.services.lambda.runtime.events.SQSEvent event context]
  (log-caller-details-sqs "handleAMISherate" context)
  (let [messages (seq (.getRecords event))]
    (doseq [^SQSEvent$SQSMessage msg messages]
      (handle-single-herate! msg))))
