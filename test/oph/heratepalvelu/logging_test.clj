(ns oph.heratepalvelu.logging_test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.eHOKSherateHandler :refer :all]
            [oph.heratepalvelu.log.caller-log :refer :all]
            [oph.heratepalvelu.common :refer :all]
            [oph.heratepalvelu.util :refer :all])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))

(use-fixtures :once clean-logs)

(deftest test-caller-log-herate-ehoks
  (testing "caller-log logs request info for eHOKSherateHandler"
    (log-caller-details "handleHOKSherate"
                        (mock-handler-event :ehoksherate)
                        (mock-handler-context))
    (is (true? (did-log? "Lambdaa handleHOKSherate kutsuttiin" "INFO")))
    (is (true? (did-log? dummy-opiskeluoikeus-oid "INFO")))
    (is (true? (did-log? (str "RequestId: " dummy-request-id) "INFO")))))

(deftest test-caller-log-herate-email
  (testing "caller-log logs request info for herateEmailHandler"
    (log-caller-details "handleSendEmails"
                        (mock-handler-event :scheduledherate)
                        (mock-handler-context))
    (is (true? (did-log? "Lambdaa handleSendEmails kutsuttiin" "INFO")))
    (is (true? (did-log? dummy-scheduled-resources "INFO")))
    (is (true? (did-log? (str "RequestId: " dummy-request-id) "INFO")))))

(deftest test-caller-log-herate-updatedopiskeluoikeus
  (testing "caller-log logs request info for herateEmailHandler"
    (log-caller-details "handleUpdatedOpiskeluoikeus"
                        (mock-handler-event :scheduledherate)
                        (mock-handler-context))
    (is (true? (did-log? "Lambdaa handleUpdatedOpiskeluoikeus kutsuttiin" "INFO")))
    (is (true? (did-log? dummy-scheduled-resources "INFO")))
    (is (true? (did-log? (str "RequestId: " dummy-request-id) "INFO")))))

(deftest test-ehoksherate-aws-service-exception
  (testing "Failed DynamoDB put throws exception and logs error"
    (with-redefs
      [oph.heratepalvelu.external.organisaatio/get-organisaatio mock-get-organisaatio
       oph.heratepalvelu.common/get-koulutustoimija-oid mock-get-koulutustoimija-oid
       oph.heratepalvelu.external.koski/get-opiskeluoikeus mock-get-opiskeluoikeus
       oph.heratepalvelu.common/check-duplicate-herate? mock-check-duplicate-herate-true?
       oph.heratepalvelu.common/check-organisaatio-whitelist? mock-check-organisaatio-whitelist-true?
       oph.heratepalvelu.db.dynamodb/put-item mock-put-item-aws-exception
       oph.heratepalvelu.external.arvo/get-kyselylinkki mock-get-kyselylinkki]
      (do
        (is (thrown? AwsServiceException (-handleHOKSherate
                                           nil
                                           (mock-handler-event :ehoksherate)
                                           (mock-handler-context))))
        (is (true? (did-log? "Virhe tietokantaan tallennettaessa" "ERROR")))
        (is (true? (did-log? "Linkki deaktivoitu" "INFO")))))))

(deftest test-ehoksherate-cond-check-exception
  (testing "Failed conditional check throws exception and logs warn"
    (with-redefs
      [oph.heratepalvelu.external.organisaatio/get-organisaatio mock-get-organisaatio
       oph.heratepalvelu.common/get-koulutustoimija-oid mock-get-koulutustoimija-oid
       oph.heratepalvelu.external.koski/get-opiskeluoikeus mock-get-opiskeluoikeus
       oph.heratepalvelu.common/check-duplicate-herate? mock-check-duplicate-herate-true?
       oph.heratepalvelu.common/check-organisaatio-whitelist? mock-check-organisaatio-whitelist-true?
       oph.heratepalvelu.db.dynamodb/put-item mock-put-item-cond-check-exception
       oph.heratepalvelu.external.arvo/get-kyselylinkki mock-get-kyselylinkki]
      (do
        (-handleHOKSherate nil (mock-handler-event :ehoksherate) (mock-handler-context))
        (is (true? (did-log? "Tämän kyselyn linkki on jo toimituksessa oppilaalle" "WARN")))
        (is (true? (did-log? "Linkki deaktivoitu" "INFO")))))))