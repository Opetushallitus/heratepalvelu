(ns oph.heratepalvelu.amis.AMISSMSHandler-test
  "Testaa AMISSMSHandleriin liittyviä funktioita."
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.amis.AMISSMSHandler :as ash]
            [oph.heratepalvelu.common :as c])
  (:import (java.time LocalDate)))

(defn- mock-query-items [params options] {:params params :options options})

(deftest test-query-lahetettavat
  (testing "Varmistaa, että query-lahetettavat toimii oikein."
    (with-redefs [oph.heratepalvelu.common/local-date-now
                  (fn [] (LocalDate/of 2022 3 3))
                  oph.heratepalvelu.db.dynamodb/query-items mock-query-items]
      (is (= (ash/query-lahetettavat 20)
             {:params
              {:sms-lahetystila [:eq [:s (:ei-lahetetty c/kasittelytilat)]]
               :alkupvm         [:le [:s "2022-03-03"]]}
              :options
              {:index "smsIndex"
               :filter-expression "attribute_exists(#kyselylinkki)"
               :expr-attr-names {"#kyselylinkki" "kyselylinkki"}
               :limit 20}})))))
