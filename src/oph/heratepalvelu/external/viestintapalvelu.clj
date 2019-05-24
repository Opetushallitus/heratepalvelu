(ns oph.heratepalvelu.external.viestintapalvelu
  (:require [clojure.tools.logging :as log]))

(defn send-email [email]
  (log/info "Sent " email))