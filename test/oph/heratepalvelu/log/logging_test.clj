(ns oph.heratepalvelu.log.logging_test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.amis.AMISherateHandler :refer :all]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.common :refer :all]
            [oph.heratepalvelu.test-util :refer :all])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))

(use-fixtures :once clean-logs)

(deftest test-caller-log-herate-ehoks
  (testing "caller-log logs request info for eHOKSherateHandler"
    (log-caller-details-sqs "handleAMISherate"
                            (mock-handler-context))
    (is (true? (did-log? "Lambdaa handleAMISherate kutsuttiin" "INFO")))
    (is (true? (did-log? (str "RequestId: " dummy-request-id) "INFO")))))

(deftest test-caller-log-herate-email
  (testing "caller-log logs request info for herateEmailHandler"
    (log-caller-details-scheduled "handleSendEmails"
                                  (mock-handler-event :scheduledherate)
                                  (mock-handler-context))
    (is (true? (did-log? "Lambdaa handleSendEmails kutsuttiin" "INFO")))
    (is (true? (did-log? dummy-scheduled-resources "INFO")))
    (is (true? (did-log? (str "RequestId: " dummy-request-id) "INFO")))))

(deftest test-caller-log-herate-updatedopiskeluoikeus
  (testing "caller-log logs request info for herateEmailHandler"
    (log-caller-details-scheduled "handleUpdatedOpiskeluoikeus"
                                  (mock-handler-event :scheduledherate)
                                  (mock-handler-context))
    (is (true? (did-log? "Lambdaa handleUpdatedOpiskeluoikeus kutsuttiin"
                         "INFO")))
    (is (true? (did-log? dummy-scheduled-resources "INFO")))
    (is (true? (did-log? (str "RequestId: " dummy-request-id) "INFO")))))

(deftest test-ehoksherate-aws-service-exception
  (testing "Failed DynamoDB put throws exception and logs error"
    (with-redefs [oph.heratepalvelu.external.organisaatio/get-organisaatio
                  mock-get-organisaatio
                  oph.heratepalvelu.common/get-koulutustoimija-oid
                  mock-get-koulutustoimija-oid
                  oph.heratepalvelu.external.koski/get-opiskeluoikeus
                  mock-get-opiskeluoikeus
                  oph.heratepalvelu.common/check-duplicate-herate?
                  mock-check-duplicate-herate-true?
                  oph.heratepalvelu.common/check-organisaatio-whitelist?
                  mock-check-organisaatio-whitelist-true?
                  oph.heratepalvelu.db.dynamodb/put-item
                  mock-put-item-aws-exception
                  oph.heratepalvelu.external.arvo/create-amis-kyselylinkki
                  mock-get-kyselylinkki
                  oph.heratepalvelu.external.arvo/delete-amis-kyselylinkki
                  mock-deactivate-kyselylinkki
                  oph.heratepalvelu.external.ehoks/get-hankintakoulutus-oids
                  mock-get-hankintakoulutus-oids-empty]
      (do
        (is (thrown? AwsServiceException (-handleAMISherate
                                           nil
                                           (mock-handler-event :ehoksherate)
                                           (mock-handler-context))))
        (is (true? (did-log? "Virhe tietokantaan tallennettaessa" "ERROR")))))))

(deftest test-ehoksherate-cond-check-exception
  (testing "Failed conditional check throws exception and logs warn"
    (with-redefs
      [oph.heratepalvelu.external.organisaatio/get-organisaatio
       mock-get-organisaatio
       oph.heratepalvelu.common/get-koulutustoimija-oid
       mock-get-koulutustoimija-oid
       oph.heratepalvelu.external.koski/get-opiskeluoikeus
       mock-get-opiskeluoikeus
       oph.heratepalvelu.common/check-duplicate-herate?
       mock-check-duplicate-herate-true?
       oph.heratepalvelu.common/check-organisaatio-whitelist?
       mock-check-organisaatio-whitelist-true?
       oph.heratepalvelu.db.dynamodb/put-item
       mock-put-item-cond-check-exception
       oph.heratepalvelu.external.arvo/create-amis-kyselylinkki
       mock-get-kyselylinkki
       oph.heratepalvelu.external.arvo/delete-amis-kyselylinkki
       mock-deactivate-kyselylinkki
       oph.heratepalvelu.external.ehoks/get-hankintakoulutus-oids
       mock-get-hankintakoulutus-oids-empty
       oph.heratepalvelu.external.ehoks/patch-amis-aloitusherate-kasitelty
       mock-patch-amis-aloitusherate-kasitelty
       oph.heratepalvelu.external.ehoks/patch-amis-paattoherate-kasitelty
       mock-patch-amis-paattoherate-kasitelty]
      (do
        (-handleAMISherate nil
                           (mock-handler-event :ehoksherate)
                           (mock-handler-context))
        (is (true? (did-log?
                     "Tämän kyselyn linkki on jo toimituksessa oppilaalle"
                     "WARN")))))))
