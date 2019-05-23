(ns oph.heratepalvelu.external.arvo)

(defn build-arvo-request-body [hoks
                               opiskeluoikeus
                               koulutustoimija
                               alkupvm
                               request-id
                               kyselytyyppi]
  (assoc {} :vastaamisajan_alkupvm          alkupvm
            :kyselyn_tyyppi                 kyselytyyppi
            :tutkintotunnus                 (-> hoks
                                                :tutkinto
                                                :nimi)
            :tutkinnon_suorituskieli        (-> opiskeluoikeus
                                        :suoritukset
                                        seq
                                        first
                                        :suorituskieli
                                        :lyhytNimi)
            :koulutustoimija_oid            koulutustoimija
            :oppilaitos_oid                 (-> opiskeluoikeus
                                                :oppilaitos
                                                :oid)
            :request-id                     request-id
            :toimipiste_oid                 nil
            :hankintakoulutuksen_toteuttaja nil))



(defn get-kyselylinkki [data]
  "https://arvovastaus.csc.fi/ABC123")
