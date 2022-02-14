(ns oph.heratepalvelu.test-util
  (:require [clojure.test :refer :all]
            [cheshire.core :refer :all]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clj-time.core :as t])
  (:import (com.amazonaws.services.lambda.runtime.events SQSEvent)
           (com.amazonaws.services.lambda.runtime.events SQSEvent$SQSMessage ScheduledEvent)
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
    (= "1.2.246.562.10.346830761111"
       (last (:organisaatio-oid conds)))
    {:organisaatio-oid "1.2.246.562.10.346830761111"
     :kayttoonottopvm (str (t/plus (t/today) (t/days 1)))}
    (= "1.2.246.562.10.346830761112"
       (last (:organisaatio-oid conds)))
    {:organisaatio-oid "1.2.246.562.10.346830761112"
     :kayttoonottopvm  (str (t/today))}))

(def dummy-opiskeluoikeus-oid "1.2.246.562.24.10442483592")
(def dummy-request-id "1d6c30bb-a2d9-5540-aa1a-65410fc2f8f5")
(def dummy-scheduled-resources "arn:aws:events:eu-west-1:123456789:rule/test-service-rule")

(def mock-herate-sqs-message
  (doto (SQSEvent$SQSMessage.)
    (.setMessageId "19dd0b57-b21e-4ac1-bd88-01bbb068cb78")
    (.setReceiptHandle "MessageReceiptHandle")
    (.setBody (generate-string
                {:kyselytyyppi "HOKS_hyvaksytty"
                 :alkupvm "2021-07-01"
                 :sahkoposti "testi@testi.fi"
                 :opiskeluoikeus-oid dummy-opiskeluoikeus-oid
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
      (.setRecords (list mock-herate-sqs-message)))
    (= :scheduledherate handler)
    (doto (ScheduledEvent.)
      (.setId "d77bcbc4-0b2b-4d45-9694-b1df99175cfb")
      (.setDetailType "Scheduled Event")
      (.setResources (list dummy-scheduled-resources)))))

(defn mock-sqs-event [item]
  (let [event (SQSEvent.)
        message (SQSEvent$SQSMessage.)]
    (.setBody message (generate-string item))
    (.setRecords event [message])
    event))

(defn reify-context
  ([] (reify-context 100))
  ([milliseconds]
    (reify com.amazonaws.services.lambda.runtime.Context
      (getAwsRequestId [this]
        dummy-request-id)
      (getRemainingTimeInMillis [this]
        milliseconds))))

(defn mock-handler-context
  ([] (reify-context))
  ([milliseconds]
   (reify-context milliseconds)))

(defn mock-get-hankintakoulutus-oids-empty [_]
  [])

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

(defn mock-check-organisaatio-whitelist-true? [_ _] true)

(defn mock-check-organisaatio-whitelist-false? [_] false)

(defn mock-check-duplicate-herate-true? [_ _ _ _] true)

(defn mock-check-duplicate-herate-false? [_ _ _ _] false)

(defn mock-get-kyselylinkki [_] {:kysely_linkki "https://kysely.linkki/12345"})

(defn mock-deactivate-kyselylinkki [_] nil)

(defn mock-patch-amis-aloitusherate-kasitelty [_] nil)

(defn mock-patch-amis-paattoherate-kasitelty [_] nil)

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


(def mock-log-file (atom []))

(defn clear-logs-before-test [f]
  (reset! mock-log-file [])
  (f))

(defn logs-contain? [obj]
  (some #(= % obj) (vec (reverse @mock-log-file))))

(defn logs-contain-matching? [level regex]
  (some #(and (= level (:level %)) (re-matches regex (:message %)))
        (vec (reverse @mock-log-file))))

;; https://stackoverflow.com/a/41823278
(defn mock-log* [logger level throwable message]
  (reset! mock-log-file (cons {:level level :message message} @mock-log-file))
  nil)
