(ns oph.heratepalvelu.eHOKSherateHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.eHOKSherateHandler :refer :all]
            [oph.heratepalvelu.common :refer :all]
            [oph.heratepalvelu.util :refer :all])
  (:import (software.amazon.awssdk.awscore.exception AwsServiceException)))

(use-fixtures :once clean-logs)

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
        (is (thrown? AwsServiceException (-handleHOKSherate nil (mock-handler-event :ehoksherate))))
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
        (-handleHOKSherate nil (mock-handler-event :ehoksherate))
        (is (true? (did-log? "Tämän kyselyn linkki on jo toimituksessa oppilaalle" "WARN")))
        (is (true? (did-log? "Linkki deaktivoitu" "INFO")))))))
