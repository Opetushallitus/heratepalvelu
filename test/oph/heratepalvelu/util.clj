(ns oph.heratepalvelu.util
  (:require [clojure.test :refer :all]
            [cheshire.core :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as string])
  (:import (com.amazonaws.services.lambda.runtime.events SQSEvent)
           (com.amazonaws.services.lambda.runtime.events SQSEvent$SQSMessage)
           (software.amazon.awssdk.awscore.exception AwsServiceException)
           (software.amazon.awssdk.services.dynamodb.model ConditionalCheckFailedException)))

(defn mock-gets [url & [options]]
  (cond
    (.endsWith url "/opiskeluoikeus/1.2.246.562.15.43634207518")
    {:status 200
     :body {:oid "1.2.246.562.15.43634207518"
            :suoritukset
                 [{:tyyppi {:koodiarvo "ammattitutkinto"}
                   :suorituskieli {:koodiarvo "FI"}
                   :koulustusmoduuli {:tunniste {:koodiarvo 123456}}}]
            :koulutustoimija {:oid "1.2.246.562.10.346830761110"}
            :oppilaitos {:oid "1.2.246.562.10.52251087186"}}}
    (.endsWith url "/opiskeluoikeus/1.2.246.562.15.43634207512")
    {:status 200
     :body {:oid "1.2.246.562.15.43634207512"
            :suoritukset
                 [{:tyyppi {:koodiarvo "ammattitutkinto"}
                   :suorituskieli {:koodiarvo "FI"}
                   :koulustusmoduuli {:tunniste {:koodiarvo 123456}}}]
            :oppilaitos {:oid "1.2.246.562.10.52251087186"}}}))

(defn mock-get-organisaatio [oid]
  (cond
    (= oid "1.2.246.562.10.52251087186")
    {:parentOid "1.2.246.562.10.346830761110"}))

(defn mock-posts [url & [options]]
  (cond
    (.endsWith url "/api/vastauslinkki/v1")
    {:kysely_linkki "https://arvovastaus.csc.fi/ABC123"}))

(defn mock-cas-posts [url body & [options]])

(defn mock-get-item-from-whitelist [conds table]
  (cond
    (= "1.2.246.562.10.346830761110"
       (last (:organisaatio-oid conds)))
    {:organisaatio-oid "1.2.246.562.10.346830761110"
     :kayttoonottopvm "2019-07-01"}
    (= "1.2.246.562.10.346830761110"
       (last (:organisaatio-oid conds)))
    {:organisaatio-oid "1.2.246.562.10.346830761111"
     :kayttoonottopvm "3019-07-01"}))

(def mock-herate-sqs-message
  (doto (SQSEvent$SQSMessage.)
    (.setMessageId "19dd0b57-b21e-4ac1-bd88-01bbb068cb78")
    (.setReceiptHandle "MessageReceiptHandle")
    (.setBody (generate-string
                {:kyselytyyppi "HOKS_hyvaksytty"
                 :alkupvm "2019-05-20"
                 :sahkoposti "testi@testi.fi"
                 :opiskeluoikeus-oid "1.2.246.562.24.10442483592"
                 :ehoks-id 1337
                 :oppija-oid "1.2.246.562.24.10442483592"}))
    (.setMd5OfBody "436c176af9d103ffaa0478f38c3091ed")
    (.setEventSource "aws:sqs")
    (.setEventSourceArn "arn:aws:sqs:eu-west-1:123456789012:MyQueue")
    (.setAwsRegion "eu-west-1")))

(defn mock-handler-event [handler]
  (cond
    (= :ehoksherate handler)
      (doto (SQSEvent.)
        (.setRecords (list mock-herate-sqs-message)))))

(defn mock-get-koulutustoimija-oid [_]
  (str "1.2.246.562.10.346830761110"))

(defn mock-get-opiskeluoikeus [_]
  {:oid "1.2.246.562.24.10442483592"
   :suoritukset [
                 {:tyyppi {:koodiarvo "ammatillinentutkinto"}
                 :suorituskieli {:koodiarvo "FI"}
                 :koulustusmoduuli {:tunniste {:koodiarvo 123456}}}]
   :koulutustoimija {:oid "1.2.246.562.10.346830761110"}
   :oppilaitos {:oid "1.2.246.562.10.52251087186"}})

(defn mock-check-organisaatio-whitelist-true? [_] true)

(defn mock-check-organisaatio-whitelist-false? [_] false)

(defn mock-check-duplicate-herate-true? [_ _ _ _] true)

(defn mock-check-duplicate-herate-false? [_ _ _ _] false)

(defn mock-get-kyselylinkki [_] "https://kysely.linkki/12345")

(defn mock-put-item-aws-exception [_ _]
  (throw (-> (AwsServiceException/builder)
             (.message "exception")
             (.build))))

(defn mock-put-item-cond-check-exception [_ _]
  (throw (-> (ConditionalCheckFailedException/builder)
             (.message "exception")
             (.build))))

(defn- log-row-has-message-and-level [row msg lvl]
  (and (string/includes? (:message row) msg)
       (string/includes? (:level row) lvl)))

(defn did-log? [msg lvl]
  (let [logs (map #(parse-string % true)
       (line-seq (clojure.java.io/reader
                   (str (System/getProperty "java.io.tmpdir") "herate/herate-test.log"))))]
    (some #(log-row-has-message-and-level % msg lvl) logs)
    ))

(defn delete-test-log-file []
  (io/delete-file (str (System/getProperty "java.io.tmpdir") "herate/herate-test.log") true))

(defn clean-logs [f]
  (f)
  (delete-test-log-file))
