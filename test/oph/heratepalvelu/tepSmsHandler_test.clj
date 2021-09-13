(ns oph.heratepalvelu.tepSmsHandler-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.tep.tepSmsHandler :as sh]))

(deftest valid-number-test
  (testing "Funktio valid-number? tunnistaa oikeita ja virheellisiä puhelinnumeroja"
    (let [fi-phone-number "040 654 3210"
          fi-phone-number-intl-fmt "040 654 3210"
          intl-phone-number "+1 517 987 5432"
          junk-invalid "laksj fdaiu fd098098asdf"
          unicode-invalid "+358 40 987 6543à"]
      (is (sh/valid-number? fi-phone-number))
      (is (sh/valid-number? fi-phone-number-intl-fmt))
      (is (sh/valid-number? intl-phone-number))
      (is (not (sh/valid-number? junk-invalid)))
      (is (not (sh/valid-number? unicode-invalid))))))
