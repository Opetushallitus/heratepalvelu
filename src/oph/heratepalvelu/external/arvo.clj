(ns oph.heratepalvelu.external.arvo
  (:require [clojure.tools.logging :as log]))

(defn build-arvo-request-body [alkupvm
                               request-id
                               kyselytyyppi
                               koulutustoimija
                               oppilaitos
                               tutkintotunnus
                               suorituskieli]
  (assoc {} :vastaamisajan_alkupvm          alkupvm
            :kyselyn_tyyppi                 kyselytyyppi
            :tutkintotunnus                 tutkintotunnus
            :tutkinnon_suorituskieli        suorituskieli
            :koulutustoimija_oid            koulutustoimija
            :oppilaitos_oid                 oppilaitos
            :request-id                     request-id
            :toimipiste_oid                 nil
            :hankintakoulutuksen_toteuttaja nil))



(defn get-kyselylinkki [data]
  (:kysely_linkki {:kysely_linkki "https://arvovastaus.csc.fi/ABC123"}))

(defn deactivate-kyselylinkki [linkki]
  (log/info "Linkki deaktivoitu"))
