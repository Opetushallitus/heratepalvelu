(ns oph.heratepalvelu.external.viestintapalvelu
  "Wrapperit ja funktiot viestien luomiseen viestintäpalvelulle."
  (:require [clojure.string :as str]
            [environ.core :refer [env]]
            [hiccup.core :as h]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.cas-client :as cas-client]))

(def horizontal-line
  "Vaakasuora linja HTML-sähköpostiviestissä."
  [:table {:cellspacing "0"
           :cellpadding "0"
           :border "0"
           :width "100%"
           :style "width: 100% !important;"}
   [:tr
    [:td {:align "left"
          :valign "top"
          :width "600px"
          :height "1"
          :style (str "background-color: #f0f0f0; border-collapse:collapse; "
                      "mso-table-lspace: 0pt; mso-table-rspace: 0pt; "
                      "mso-line-height-rule: exactly; line-height: 1px;")}]]])

(defn- amispalaute-body-alkukysely
  "Luo AMIS-aloituskyselyviestin tekstisisällön."
  [link]
  [:div
   [:p "Hyvä opiskelija!"]
   [:p (str "Henkilökohtainen osaamisen kehittämissuunnitelmasi (HOKS) on nyt "
            "hyväksytty ja opinnot alkamassa. Onnistunut aloitus ja "
            "suunnitelma luovat hyvän pohjan tavoitteiden saavuttamiselle. "
            "Siksi kokemuksesi ovat tärkeitä. Kerro meille, missä olemme "
            "onnistuneet ja mitä voisimme tehdä vielä paremmin.")]
   [:p (str "Toivomme, että käyttäisit noin 10-15 minuuttia aikaa tähän "
            "kyselyyn vastaamiseen. Kysymykset koskevat ammatillisen "
            "koulutuksen aloitusvaihetta, HOKSin laadintaa ja "
            "opiskeluilmapiiriä.")]
   [:p (str "Palaute annetaan nimettömänä. Vastauksiasi käytetään ammatillisen "
            "koulutuksen kehittämiseen.")]
   [:p [:a {:href link} link]]
   [:p "Jos linkki ei avaudu, kopioi linkki selaimesi osoiteriville."]
   [:p "Terveisin oppilaitoksesi"]
   horizontal-line
   [:p "Bästa studerande!"]
   [:p (str "Din personliga utvecklingsplan för kunnandet (PUK) har nu "
            "godkänts och studierna ska snart börja. En lyckad studiestart och "
            "god planering skapar en bra grund för att du ska kunna nå dina "
            "mål. Därför är det viktigt att vi får höra dina erfarenheter och "
            "kommentarer. Vi vill gärna höra dina åsikter om hur vi har "
            "lyckats och vad vi kunde göra bättre i fortsättningen.")]
   [:p (str "Vi hoppas att du tar dig tid att svara på den här enkäten. Det "
            "tar ungefär 10–15 minuter att svara. Frågorna berör de inledande "
            "studierna, uppgörandet av PUK och studieklimatet.")]
   [:p (str "Responsen ges anonymt. Dina svar används för att utveckla "
            "yrkesutbildningen.")]
   [:p [:a {:href link} link]]
   [:p (str "Om du kan inte öppna länken, kopiera (och klistra in) länken i "
            "webbläsarens adressfält.")]
   [:p "Hälsningar, din läroanstalt"]
   horizontal-line
   [:p "Dear student!"]
   [:p (str "Your personal competence development plan has now been approved "
            "and you are about to begin your studies. A successful beginning "
            "and a sound plan lay a good foundation for achieving your goals. "
            "This is why your experiences matter. Tell us what we did well and "
            "what we could do even better.")]
   [:p (str "We ask you to take 10 to 15 minutes to respond to this survey. "
            "The questions concern the starting phase of vocational education "
            "and training, the preparation of your personal competence plan, "
            "and the educational atmosphere.")]
   [:p (str "Your feedback will be collected anonymously. Your responses will "
            "be used to improve vocational education and training.")]
   [:p [:a {:href link} link]]
   [:p (str "If you cannot open the link, copy (and paste) the link onto the "
            "address bar of your browser.")]
   [:p "With best regards, your educational institution"]])

(defn- amispalaute-body-loppukysely
  "Luo AMIS-loppukyselyviestin tekstisisällön."
  [link]
  [:div
   [:p "Hyvä opiskelija!"]
   [:p "Olet edennyt hienosti ja saamassa päätökseen tavoitteesi mukaiset ammatilliset opinnot."]
   [:p (str "Kokemuksesi koulutuksesta ovat tärkeitä. Kerro meille, missä "
            "olemme onnistuneet ja mitä voisimme tehdä vielä paremmin.")]
   [:p (str "Toivomme, että käyttäisit noin 10-15 minuuttia aikaa tähän "
            "kyselyyn vastaamiseen.")]
   [:p (str "Palaute annetaan nimettömänä. Vastauksiasi käytetään ammatillisen "
            "koulutuksen kehittämiseen.")]
   [:p "Kiitos, että vastaat!"]
   [:p [:a {:href link} link]]
   [:p "Jos linkki ei avaudu, kopioi linkki selaimesi osoiteriville."]
   [:p "Terveisin oppilaitoksesi"]
   horizontal-line
   [:p "Bästa studerande!"]
   [:p "Du har utvecklats väl och är på väg att slutföra dina yrkesinriktade studier."]
   [:p (str "Dina erfarenheter av utbildningen är viktiga för oss. Vi vill "
            "gärna höra dina åsikter om hur vi har lyckats och vad vi kunde "
            "göra bättre i fortsättningen.")]
   [:p (str "Vi hoppas att du tar dig tid att svara på den här enkäten. Det "
            "tar ungefär 10–15 minuter att svara.")]
   [:p (str "Responsen ges anonymt. Dina svar används för att utveckla "
            "yrkesutbildningen.")]
   [:p "Tack för att du svarar!"]
   [:p [:a {:href link} link]]
   [:p (str "Om du kan inte öppna länken, kopiera (och klistra in) länken i "
            "webbläsarens adressfält.")]
   [:p "Hälsningar, din läroanstalt"]
   horizontal-line
   [:p "Dear student!"]
   [:p "You have progressed well and are about to complete the vocational studies you set as your goal."]
   [:p (str "Your experiences in education and training matter. Tell us what "
            "we did well and what we could do even better.")]
   [:p "We ask you to take 10 to 15 minutes to respond to this survey."]
   [:p (str "Your feedback will be collected anonymously. Your responses will "
            "be used to improve vocational education and training.")]
   [:p "Thank you for responding!"]
   [:p [:a {:href link} link]]
   [:p (str "If you cannot open the link, copy (and paste) the link onto the "
            "address bar of your browser.")]
   [:p "With best regards, your educational institution"]])

(defn- amispalaute-body
  "Luo AMIS-kyselyviestin tekstin. Data-objektiin kuuluvat seuraavat kentät:
    :kyselytyyppi   - aloittaneet, tutkinnon_suorittaneet tai
                      tutkinnon_osia_suorittaneet
    :kyselylinkki   - kyselylinkki, joka lähetetään opiskelijalle"
  [data]
  (cond
    (= (:kyselytyyppi data) "aloittaneet")
    (amispalaute-body-alkukysely (:kyselylinkki data))
    (= (:kyselytyyppi data) "tutkinnon_suorittaneet")
    (amispalaute-body-loppukysely (:kyselylinkki data))
    (= (:kyselytyyppi data) "tutkinnon_osia_suorittaneet")
    (amispalaute-body-loppukysely (:kyselylinkki data))))

(defn- amismuistutus-body
  "Luo AMIS-muistutusviestin tekstin."
  [link]
  [:div
   [:p (str "Olethan muistanut antaa palautetta oppilaitokselle!<br/>"
            "Kom ihåg att ge respons till läroanstalten!<br/>"
            "Please remember to give feedback to the educational institution!")]
   [:p [:a {:href link} link]]
   [:p (str "Jos linkki ei avaudu, kopioi linkki selaimesi osoiteriville.<br/>"
            "Om du kan inte öppna länken, kopiera (och klistra in) länken i "
            "webbläsarens adressfält.<br/>"
            "If you cannot open the link, copy (and paste) the link onto the "
            "address bar of your browser.<br/>")]
   horizontal-line])

(defn amispalaute-html
  "Luo kokonaisen AMIS-kyselyviestin HTML-bodyn. Dataan kuuluvat nämä kentät:
    :kyselytyyppi   - aloittaneet, tutkinnon_suorittaneet tai
                      tutkinnon_osia_suorittaneet
    :kyselylinkki   - kyselylinkki, joka lähetetään opiskelijalle
    :suorituskieli  - tutkinnon kieli kahden kirjaimein koodina"
  [data]
  (str "<!DOCTYPE html>"
       (h/html [:html {:lang (:suorituskieli data)}
                [:head [:meta {:charset "UTF-8"}]]
                [:body (amispalaute-body data)]])))

(defn amismuistutus-html
  "Luo kokonaisen AMIS-kyselyn muistutusviestin HTML-bodyn. Dataan kuuluvat:
    :kyselylinkki   - kyselylinkki, joka lähetetään opiskelijalle
    :suorituskieli  - tutkinnon kieli kahden kirjaimein koodina"
  [data]
  (str "<!DOCTYPE html>"
       (h/html [:html {:lang (:suorituskieli data)}
                [:head [:meta {:charset "UTF-8"}]]
                [:body
                 (amismuistutus-body (:kyselylinkki data))
                 (amispalaute-body data)]])))

(defn- tyopaikkaohjaaja-body
  "Luo työpaikkaohjaajan kyselyviestin tekstisisällön. Data-objektiin täytyy
  kuulua vain kyselylinkki (:kyselylinkki -avaimen arvona), ja oppilaitokset
  ovat lista objekteja, joista jokaisessa on oppilaitoksen nimi kolmeksi eri
  kieleksi (avaimet :en, :fi, ja :sv)."
  [data oppilaitokset]
  [:div
   [:p [:b "Hyvä työpaikkaohjaaja!"]]
   [:p (str "Kiitos koulutussopimus-/oppisopimusopiskelijoiden ohjaamisesta! "
            "Tehdään yhdessä osaajia työelämään.")]
   [:p (str "Pyydämme vastaamaan tähän kyselyyn (5 min) yhteistyömme "
            "kehittämiseksi. Haluamme kuulla kokemuksianne ohjaustyöstä ja "
            "yhteistyöstä oppilaitoksen kanssa. Kyselyssä ei arvioida "
            "opiskelijaa. Kyselyn voi siirtää työpaikallanne toiselle "
            "henkilölle, jos hän on käytännössä enemmän ohjannut opiskelijaa. "
            "Vastaajan henkilötietoja ei kysytä.")]
   [:p "Palautteenne on tärkeä, kiitos, että vastaat!"]
   [:p [:a {:href (:kyselylinkki data)} (:kyselylinkki data)]]
   [:p "Jos linkki ei avaudu, kopioi linkki selaimesi osoiteriville."]
   [:p "Ystävällisin terveisin,"]
   [:p (str/join ", " (map :fi oppilaitokset))]
   [:p "Osoitelähde: Opetushallituksen (OPH) eHOKS-rekisteri"]
   horizontal-line
   [:p [:b "Bästa arbetsplatshandledare!"]]
   [:p (str "Tack för att Ni handleder studerande på utbildnings-/läroavtal! "
            "Tillsammans skapar vi experter för arbetslivet. ")]
   [:p (str "Vi ber er svara på den här enkäten (5 min) för att utveckla vårt "
            "samarbete. Vi vill lyssna in er erfarenheter av det dagliga "
            "handledningsarbetet och samarbetet med utbildningsanordnaren. "
            "Studerande utvärderas inte i enkäten. Enkäten kan vidarebefordras "
            "till den person som i praktiken har deltagit mer i handledningen "
            "av den studerande. Respondentens personliga information "
            "efterfrågas inte.")]
   [:p "Din respons är viktig, tack för att du svarar!"]
   [:p [:a {:href (:kyselylinkki data)} (:kyselylinkki data)]]
   [:p (str "Om du kan inte öppna länken, kopiera (och klistra in) länken i "
            "webbläsarens adressfält.")]
   [:p "Med vänliga hälsningar,"]
   [:p (str/join ", " (map #(or (:sv %1) (:fi %1)) oppilaitokset))]
   [:p "Adresskälla: Utbildningsstyrelsens (UBS) ePUK-registret"]
   horizontal-line
   [:p [:b "Dear workplace instructor!"]]
   [:p (str "Thank you for guiding students with a training "
            "agreement/apprenticeship! Let’s make experts for the workforce "
            "together.")]
   [:p (str "Please respond to this survey (5 min) to help us work together "
            "more effectively. We would like to hear about your experiences "
            "guiding students and working with the educational institution. "
            "The survey does not assess the student. The survey can be "
            "forwarded to another person at your workplace if he or she has "
            "guided the student more. Respondent´s personal information is not "
            "requested.")]
   [:p "Your feedback is important, thank you for responding!"]
   [:p [:a {:href (:kyselylinkki data)} (:kyselylinkki data)]]
   [:p (str "If you cannot open the link, copy (and paste) the link onto the "
            "address bar of your browser.")]
   [:p "With best regards,"]
   [:p (str/join ", " (map #(or (:en %1) (:fi %1)) oppilaitokset))]
   [:p "Address source: Opetushallituksen (OPH) eHOKS-register"]])

(defn- tyopaikkaohjaaja-muistutus-body
  "Luo työpaikkaohjaajan muistutusviestin tekstisisällön. Data-objektiin täytyy
  kuulua vain kyselylinkki (:kyselylinkki -avaimen arvona), ja oppilaitokset
  ovat lista objekteja, joista jokaisessa on oppilaitoksen nimi kolmeksi eri
  kieleksi (avaimet :en, :fi, ja :sv)."
  [data oppilaitokset]
  [:div
   [:p (str "Olethan muistanut antaa palautetta oppilaitokselle!<br/>"
            "Kom ihåg att ge respons till läroanstalten!<br/>"
            "Please remember to give feedback to the educational institution!")]
   [:p [:a {:href (:kyselylinkki data)} (:kyselylinkki data)]]
   [:p (str "Jos linkki ei avaudu, kopioi linkki selaimesi osoiteriville.<br/>"
            "Om du kan inte öppna länken, kopiera (och klistra in) länken i "
            "webbläsarens adressfält.<br/>"
            "If you cannot open the link, copy (and paste) the link onto the "
            "address bar of your browser.<br/>")]
   [:p (str "Kiitos, että vastaat - Tack för att du svarar – Thank you for "
            "responding!")]
   [:p (str/join ", " (map :fi oppilaitokset))]
   [:p [:b "Hyvä työpaikkaohjaaja!"]]
   [:p (str "Kiitos koulutussopimus-/oppisopimusopiskelijoiden ohjaamisesta! "
            "Tehdään yhdessä osaajia työelämään.")]
   [:p (str "Pyydämme vastaamaan tähän kyselyyn (5 min) yhteistyömme "
            "kehittämiseksi. Haluamme kuulla kokemuksianne ohjaustyöstä ja "
            "yhteistyöstä oppilaitoksen kanssa. Kyselyssä ei arvioida "
            "opiskelijaa. Kyselyn voi siirtää työpaikallanne toiselle "
            "henkilölle, jos hän on käytännössä enemmän ohjannut opiskelijaa. "
            "Vastaajan henkilötietoja ei kysytä.")]
   [:p "Palautteenne on tärkeä, kiitos, että vastaat!"]
   [:p [:a {:href (:kyselylinkki data)} (:kyselylinkki data)]]
   [:p "Jos linkki ei avaudu, kopioi linkki selaimesi osoiteriville."]
   [:p "Ystävällisin terveisin,"]
   [:p (str/join ", " (map :fi oppilaitokset))]
   [:p "Osoitelähde: Opetushallituksen (OPH) eHOKS-rekisteri"]
   horizontal-line
   [:p [:b "Bästa arbetsplatshandledare!"]]
   [:p (str "Tack för att Ni handleder studerande på utbildnings-/läroavtal! "
            "Tillsammans skapar vi experter för arbetslivet. ")]
   [:p (str "Vi ber er svara på den här enkäten (5 min) för att utveckla vårt "
            "samarbete. Vi vill lyssna in er erfarenheter av det dagliga "
            "handledningsarbetet och samarbetet med utbildningsanordnaren. "
            "Studerande utvärderas inte i enkäten. Enkäten kan vidarebefordras "
            "till den person som i praktiken har deltagit mer i handledningen "
            "av den studerande. Respondentens personliga information "
            "efterfrågas inte.")]
   [:p "Din respons är viktig, tack för att du svarar!"]
   [:p [:a {:href (:kyselylinkki data)} (:kyselylinkki data)]]
   [:p (str "Om du kan inte öppna länken, kopiera (och klistra in) länken i "
            "webbläsarens adressfält.")]
   [:p "Med vänliga hälsningar,"]
   [:p (str/join ", " (map #(or (:sv %1) (:fi %1)) oppilaitokset))]
   [:p "Adresskälla: Utbildningsstyrelsens (UBS) ePUK-registret"]
   horizontal-line
   [:p [:b "Dear workplace instructor!"]]
   [:p (str "Thank you for guiding students with a training "
            "agreement/apprenticeship! Let’s make experts for the workforce "
            "together.")]
   [:p (str "Please answer this survey (5 min) to help us work together more "
            "effectively. We would like to hear about your experiences guiding "
            "students and working with the educational institution. The survey "
            "does not assess the student. The survey can be forwarded to "
            "another person at your workplace if he or she has guided the "
            "student more. Respondent´s personal information is not "
            "requested.")]
   [:p "Your feedback is important, thank you for responding!"]
   [:p [:a {:href (:kyselylinkki data)} (:kyselylinkki data)]]
   [:p (str "If you cannot open the link, copy (and paste) the link onto the "
            "address bar of your browser.")]
   [:p "With best regards,"]
   [:p (str/join ", " (map #(or (:en %1) (:fi %1)) oppilaitokset))]
   [:p "Address source: Opetushallituksen (OPH) eHOKS-register"]])

(defn tyopaikkaohjaaja-html
  "Luo kokonaisen työpaikkaohjaajan kyselyviestin HTML-bodyn. Data-objektiin
  täytyy kuulua vain kyselylinkki (:kyselylinkki -avaimen arvona), ja
  oppilaitokset ovat lista objekteja, joista jokaisessa on oppilaitoksen nimi
  kolmeksi eri kieleksi (avaimet :en, :fi, ja :sv)."
  [data oppilaitokset]
  (str "<!DOCTYPE html>"
       (h/html [:html {:lang "FI"}
                [:head [:meta {:charset "UTF-8"}]]
                [:body (tyopaikkaohjaaja-body data oppilaitokset)]])))

(defn tyopaikkaohjaaja-muistutus-html
  "Luo kokonaisen työpaikkaohjaajan muistutusviestin HTML-bodyn. Data-objektiin
  täytyy kuulua vain kyselylinkki (:kyselylinkki -avaimen arvona), ja
  oppilaitokset ovat lista objekteja, joista jokaisessa on oppilaitoksen nimi
  kolmeksi eri kieleksi (avaimet :en, :fi, ja :sv)."
  [data oppilaitokset]
  (str "<!DOCTYPE html>"
       (h/html [:html {:lang "FI"}
                [:head [:meta {:charset "UTF-8"}]]
                [:body (tyopaikkaohjaaja-muistutus-body data oppilaitokset)]])))

(defn send-email
  "Lähettää sähköpostin viestintäpalveluun. Parametri 'email' on map, johon
  kuuluvat seuraavat kentät:
    :address  - vastaanottajan sähköpostiosoite
    :subject  - sähköpostin aihe
    :sender   - viestin lähettäjä
    :body     - viestin HTML-body"
  [email]
  (let [resp (cas-client/cas-authenticated-post
               (:viestintapalvelu-url env)
               {:recipient [{:email (:address email)}]
                :email {:callingProcess "heratepalvelu"
                        :from "no-reply@opintopolku.fi"
                        :sender (:sender email)
                        :subject (:subject email)
                        :isHtml true
                        :body (:body email)}}
               {:as :json})]
    (:body resp)))

(defn get-email-status
  "Hakee sähköpostilähetyksen tilan viestintäpalvelusta ID:n perusteella."
  [id]
  (:body (cas-client/cas-authenticated-post
           (str (:viestintapalvelu-url env) "/status")
           id
           {:as :json})))

(defn convert-email-status
  "Muuttaa viestintäpalvelusta palautetun statuksen käsittelytilaksi."
  [status]
  (if (= (:numberOfSuccessfulSendings status) 1)
    (:success c/kasittelytilat)
    (if (= (:numberOfBouncedSendings status) 1)
      (:bounced c/kasittelytilat)
      (when (= (:numberOfFailedSendings status) 1)
        (:failed c/kasittelytilat)))))
