(ns oph.heratepalvelu.external.viestintapalvelu
  (:require [oph.heratepalvelu.external.cas-client :refer [cas-authenticated-post]]
            [environ.core :refer [env]]
            [clojure.string :as str])
  (:use hiccup.core))

(def horizontal-line
  [:table {:cellspacing "0" :cellpadding "0" :border "0" :width "100%" :style "width: 100% !important;"}
   [:tr
    [:td {:align "left" :valign "top" :width "600px" :height "1" :style "background-color: #f0f0f0; border-collapse:collapse; mso-table-lspace: 0pt; mso-table-rspace: 0pt; mso-line-height-rule: exactly; line-height: 1px;"}]]])

(defn- amispalaute-body-alkukysely [link]
  [:div
   [:p "Hyvä opiskelija!"]
   [:p "Henkilökohtainen osaamisen kehittämissuunnitelmasi (HOKS) on nyt hyväksytty ja opinnot alkamassa. Onnistunut aloitus ja suunnitelma luovat hyvän pohjan tavoitteiden saavuttamiselle. Siksi kokemuksesi ovat tärkeitä. Kerro meille, missä olemme onnistuneet ja mitä voisimme tehdä vielä paremmin."]
   [:p "Toivomme, että käyttäisit noin 10-15 minuuttia aikaa tähän kyselyyn vastaamiseen. Kysymykset koskevat ammatillisen koulutuksen aloitusvaihetta, HOKSin laadintaa ja opiskeluilmapiiriä."]
   [:p "Palaute annetaan nimettömänä. Vastauksiasi käytetään ammatillisen koulutuksen kehittämiseen."]
   [:p [:a {:href link} link]]
   [:p "Terveisin oppilaitoksesi"]
   horizontal-line
   [:p "Bästa studerande!"]
   [:p "Din personliga utvecklingsplan för kunnandet (PUK) har nu godkänts och studierna ska snart börja. En lyckad studiestart och god planering skapar en bra grund för att du ska kunna nå dina mål. Därför är det viktigt att vi får höra dina erfarenheter och kommentarer. Vi vill gärna höra dina åsikter om hur vi har lyckats och vad vi kunde göra bättre i fortsättningen."]
   [:p "Vi hoppas att du tar dig tid att svara på den här enkäten. Det tar ungefär 10–15 minuter att svara. Frågorna berör de inledande studierna, uppgörandet av PUK och studieklimatet."]
   [:p "Responsen ges anonymt. Dina svar används för att utveckla yrkesutbildningen."]
   [:p [:a {:href link} link]]
   [:p "Hälsningar, din läroanstalt"]
   horizontal-line
   [:p "Dear student!"]
   [:p "Your personal competence development plan has now been approved and you are about to begin your studies. A successful beginning and a sound plan lay a good foundation for achieving your goals. This is why your experiences matter. Tell us what we did well and what we could do even better."]
   [:p "We would like you to spend some 10 to 15 minutes on responding to this survey. The questions concern the starting phase of vocational education and training, the preparation of your personal competence plan, and the study atmosphere."]
   [:p "You can give your feedback anonymously. Your responses will be used to develop vocational education and training."]
   [:p [:a {:href link} link]]
   [:p "With best regards, your educational institution"]])

(defn- amispalaute-body-loppukysely [link]
  [:div
   [:p "Hyvä valmistunut!"]
   [:p "Onneksi olkoon! Olet saanut päätökseen tavoitteesi mukaiset ammatilliset opinnot."]
   [:p "Kokemuksesi koulutuksesta ovat tärkeitä. Kerro meille, missä olemme onnistuneet ja mitä voisimme tehdä vielä paremmin."]
   [:p "Toivomme, että käyttäisit noin 10-15 minuuttia aikaa tähän kyselyyn vastaamiseen."]
   [:p "Palaute annetaan nimettömänä. Vastauksiasi käytetään ammatillisen koulutuksen kehittämiseen."]
   [:p "Kiitos, että vastaat!"]
   [:p [:a {:href link} link]]
   [:p "Terveisin oppilaitoksesi"]
   horizontal-line
   [:p "Hej!"]
   [:p "Grattis, du har slutfört dina yrkesinriktade studier."]
   [:p "Dina erfarenheter av utbildningen är viktiga för oss. Vi vill gärna höra dina åsikter om hur vi har lyckats och vad vi kunde göra bättre i fortsättningen."]
   [:p "Vi hoppas att du tar dig tid att svara på den här enkäten. Det tar ungefär 10–15 minuter att svara."]
   [:p "Responsen ges anonymt. Dina svar används för att utveckla yrkesutbildningen."]
   [:p "Tack för att du svarar!"]
   [:p [:a {:href link} link]]
   [:p "Hälsningar, din läroanstalt"]
   horizontal-line
   [:p "Dear graduate!"]
   [:p "Congratulations! You have now finished the vocational studies you set as your goal."]
   [:p "Your experiences of education and training matter. Tell us what we did well and what we could do even better."]
   [:p "We would like you to spend some 10 to 15 minutes on responding to this survey."]
   [:p "You can give your feedback anonymously. Your responses will be used to develop vocational education and training."]
   [:p "Thank you for responding!"]
   [:p [:a {:href link} link]]
   [:p "With best regards, your educational institution"]])

(defn- amispalaute-body [data]
  (cond
    (= (:kyselytyyppi data) "aloittaneet")
    (amispalaute-body-alkukysely (:kyselylinkki data))
    (= (:kyselytyyppi data) "tutkinnon_suorittaneet")
    (amispalaute-body-loppukysely (:kyselylinkki data))
    (= (:kyselytyyppi data) "tutkinnon_osia_suorittaneet")
    (amispalaute-body-loppukysely (:kyselylinkki data))))

(defn- amismuistutus-body [link]
  [:div
   [:p (str "Olethan muistanut antaa palautetta oppilaitokselle!<br/>"
            "Kom ihåg att ge respons till läroanstalten!<br/>"
            "Please remember to give feedback to educational institution!")]
   [:p [:a {:href link} link]]
   horizontal-line])

(defn amispalaute-html [data]
  (str "<!DOCTYPE html>"
       (html [:html {:lang (:suorituskieli data)}
              [:head
               [:meta {:charset "UTF-8"}]]
              [:body
               (amispalaute-body data)]])))

(defn amismuistutus-html [data]
  (str "<!DOCTYPE html>"
       (html [:html {:lang (:suorituskieli data)}
              [:head
               [:meta {:charset "UTF-8"}]]
              [:body
               (amismuistutus-body (:kyselylinkki data))
               (amispalaute-body data)]])))

(defn tyopaikkaohjaaja-html [data oppilaitokset]
  (str "<!DOCTYPE html>"
       (html [:html {:lang "FI"}
              [:head
               [:meta {:charset "UTF-8"}]]
              [:body
               [:div
                [:p "Tässä lähettämämme viesti työpaikkaohjaajan kyselyyn vielä uudelleen, koska aiemmin lähettämästämme viestistä puuttui vastauslinkki. Pääset vastaamaan kyselyyn alla olevan linkin kautta. Pahoittelemme tuplalähetystä."]
                [:p [:b "Hyvä työpaikkaohjaaja!"]]
                [:p "Kiitos, että olet ohjannut oppisopimus- ja/tai koulutussopimusopiskelijoita. Kerro meille, miten yhteistyömme on toiminut, missä olemme onnistuneet ja mitä voisimme tehdä vielä paremmin. Toivomme, että käyttäisit noin 5 minuuttia aikaa tähän kyselyyn vastaamiseen. Vastauksesi on tärkeä ja sitä käytetään  ammatillisen koulutuksen kehittämiseen."]
                [:p "Kiitos, että vastaat."]
                [:p (str/join ", " (map :fi oppilaitokset))]
                [:p "Työpaikkaohjaajakysely: " [:a {:href (:kyselylinkki data)} (:kyselylinkki data)]]
                [:p "Tämä on valtakunnallinen palautekysely, jonka tekevät yhteistyössä Opetushallitus ja ammatilliset oppilaitokset kaikille työpaikoille, joissa on ollut oppisopimus- tai koulutussopimusopiskelijoita.  Osoitelähde: Opetushallituksen eHOKS-rekisteri."]
                horizontal-line
                [:p "Här kommer meddelandet om enkäten till arbetsplatshandledare en gång till eftersom svarslänken saknades i vårt förra meddelande. Du kan svara på enkäten genom länken nedan. Vi beklagar det dubbla meddelandet."]
                [:p [:b "Bästa arbetsplatshandledaren!"]]
                [:p "Tack för att du har handlett läroavtals- och/eller utbildningsavtalsstuderanden. Berätta för oss om hur vårt samarbete har fungerat, var vi har lyckats med och vad kan vi göra ännu bättre. Vi hoppas att du använder 5 minuter av din tid för att svara på den här enkäten. Ditt svar är viktigt och kommer att användas för att utveckla yrkesutbildningen."]
                [:p "Tack för att du svarar."]
                [:p (str/join ", " (map #(or (:sv %1) (:fi %1)) oppilaitokset))]
                [:p "Enkät till arbetsplatshandledaren: " [:a {:href (:kyselylinkki data)} (:kyselylinkki data)]]
                [:p "Detta är en nationell enkät som genomförs i samarbete med Utbildningsstyrelsen och yrkesläroanstalternas till alla arbetsplatser som har haft läroavtals- eller utbildningsavtalsstuderande. Addresskälla: Utbildningsstyrelssens ePUK-register."]
                horizontal-line
                [:p "Here is the email regarding the survey for workplace instructors once more. The original message we sent did not include a reply link. You can answer the survey by clicking on the link below. We apologize for the multiple emails."]
                [:p [:b "Dear workplace instructor!"]]
                [:p "Thank you for guiding the apprentices and/or training agreement student´s learning. Tell us how our collaboration has worked, where we have succeeded and what we could do even better. We would like you to spend some 5 minutes on responding to this survey. Your experiences matter and your responses will be used to develop vocational education and training."]
                [:p "Thank you for responding."]
                [:p (str/join ", " (map #(or (:en %1) (:fi %1)) oppilaitokset))]
                [:p "Survey to workplace instructors: " [:a {:href (:kyselylinkki data)} (:kyselylinkki data)]]
                [:p "This is a national feedback survey conducted in collaboration with National Agency for Education and Training (EDUFI) and Vocational Education and Training Institutions for all workplaces that have had students in apprenticeship training or training agreement training. Address source: EDUFI's eHOKS-register."]]]])))

(defn send-email [email]
  "Send email to viestintäpalvelu, parameter 'email' is a map containing keys
  :address (recipient email address)
  :subject (email subject)
  :sender (sender name)
  :body (message body html)"
  (let [resp (cas-authenticated-post
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

(defn get-email-status [id]
  (:body (cas-authenticated-post
           (str (:viestintapalvelu-url env) "/status")
           id
           {:as :json})))
