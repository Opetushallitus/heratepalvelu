(ns oph.heratepalvelu.external.viestintapalvelu-test
  (:require [clojure.test :refer :all]
            [oph.heratepalvelu.common :as c]
            [oph.heratepalvelu.external.viestintapalvelu :as vp]))

(deftest test-viestintapalvelu-status->kasittelytila
  (testing "viestintäpalvelu email status response to internal tila"
    (are [kasittelytila status]
         (= kasittelytila (vp/viestintapalvelu-status->kasittelytila status))
      (:success c/kasittelytilat) {:numberOfSuccessfulSendings 1
                                   :numberOfBouncedSendings 0
                                   :numberOfFailedSendings 0}
      (:bounced c/kasittelytilat) {:numberOfSuccessfulSendings 0
                                   :numberOfBouncedSendings 1
                                   :numberOfFailedSendings 0}
      (:failed c/kasittelytilat) {:numberOfSuccessfulSendings 0
                                  :numberOfBouncedSendings 0
                                  :numberOfFailedSendings 1}
      nil {:numberOfSuccessfulSendings 0
           :numberOfBouncedSendings 0
           :numberOfFailedSendings 0})))

(def mock-amispalaute-html-alkukysely
  (str
    "<!DOCTYPE html><html lang=\"fi\"><head><meta charset=\"UTF-8\" /></head>"
    "<body><div><p>Hyvä opiskelija!</p><p>Henkilökohtainen osaamisen "
    "kehittämissuunnitelmasi (HOKS) on nyt hyväksytty ja opinnot alkamassa. "
    "Onnistunut aloitus ja suunnitelma luovat hyvän pohjan tavoitteiden "
    "saavuttamiselle. Siksi kokemuksesi ovat tärkeitä. Kerro meille, missä "
    "olemme onnistuneet ja mitä voisimme tehdä vielä paremmin.</p><p>Toivomme, "
    "että käyttäisit noin 10-15 minuuttia aikaa tähän kyselyyn vastaamiseen. "
    "Kysymykset koskevat ammatillisen koulutuksen aloitusvaihetta, HOKSin "
    "laadintaa ja opiskeluilmapiiriä.</p><p>Palaute annetaan nimettömänä. "
    "Vastauksiasi käytetään ammatillisen koulutuksen kehittämiseen.</p>"
    "<p><a href=\"https://kysely.linkki/123?t=e\">"
    "https://kysely.linkki/123?t=e</a></p><p>Jos linkki ei avaudu, kopioi "
    "linkki selaimesi osoiteriville.</p><p>Terveisin oppilaitoksesi</p>"
    "<table border=\"0\" cellpadding=\"0\" cellspacing=\"0\" "
    "style=\"width: 100% !important;\" width=\"100%\"><tr><td align=\"left\" "
    "height=\"1\" style=\"background-color: #f0f0f0; border-collapse:collapse; "
    "mso-table-lspace: 0pt; mso-table-rspace: 0pt; "
    "mso-line-height-rule: exactly; line-height: 1px;\" valign=\"top\" "
    "width=\"600px\"></td></tr></table><p>Bästa studerande!</p><p>Din "
    "personliga utvecklingsplan för kunnandet (PUK) har nu godkänts och "
    "studierna ska snart börja. En lyckad studiestart och god planering skapar "
    "en bra grund för att du ska kunna nå dina mål. Därför är det viktigt att "
    "vi får höra dina erfarenheter och kommentarer. Vi vill gärna höra dina "
    "åsikter om hur vi har lyckats och vad vi kunde göra bättre i "
    "fortsättningen.</p><p>Vi hoppas att du tar dig tid att svara på den här "
    "enkäten. Det tar ungefär 10–15 minuter att svara. Frågorna berör de "
    "inledande studierna, uppgörandet av PUK och studieklimatet.</p>"
    "<p>Responsen ges anonymt. Dina svar används för att utveckla "
    "yrkesutbildningen.</p><p><a href=\"https://kysely.linkki/123?t=e\">"
    "https://kysely.linkki/123?t=e</a></p><p>Om du kan inte öppna länken, "
    "kopiera (och klistra in) länken i webbläsarens adressfält.</p>"
    "<p>Hälsningar, din läroanstalt</p><table border=\"0\" cellpadding=\"0\" "
    "cellspacing=\"0\" style=\"width: 100% !important;\" width=\"100%\"><tr>"
    "<td align=\"left\" height=\"1\" style=\"background-color: #f0f0f0; "
    "border-collapse:collapse; mso-table-lspace: 0pt; mso-table-rspace: 0pt; "
    "mso-line-height-rule: exactly; line-height: 1px;\" valign=\"top\" "
    "width=\"600px\"></td></tr></table><p>Dear student!</p><p>Your personal "
    "competence development plan has now been approved and you are about to "
    "begin your studies. A successful beginning and a sound plan lay a good "
    "foundation for achieving your goals. This is why your experiences matter. "
    "Tell us what we did well and what we could do even better.</p><p>We ask "
    "you to take 10 to 15 minutes to respond to this survey. The questions "
    "concern the starting phase of vocational education and training, the "
    "preparation of your personal competence plan, and the educational "
    "atmosphere.</p><p>Your feedback will be collected anonymously. Your "
    "responses will be used to improve vocational education and training.</p>"
    "<p><a href=\"https://kysely.linkki/123?t=e\">"
    "https://kysely.linkki/123?t=e</a></p><p>If you cannot open the link, copy "
    "(and paste) the link onto the address bar of your browser.</p><p>With "
    "best regards, your educational institution</p></div></body></html>"))

(deftest test-amispalaute-html
  (testing "amispalaute-html formats correctly alkukysely"
    (is (= (vp/amispalaute-html
             {:suorituskieli "fi"
              :kyselytyyppi "aloittaneet"
              :kyselylinkki "https://kysely.linkki/123"})
           mock-amispalaute-html-alkukysely))))

(def mock-amispalaute-html-loppukysely
  (str
    "<!DOCTYPE html><html lang=\"fi\"><head><meta charset=\"UTF-8\" /></head>"
    "<body><div><p>Hyvä opiskelija!</p><p>Olet edennyt hienosti ja saamassa "
    "päätökseen tavoitteesi mukaiset ammatilliset opinnot.</p><p>Kokemuksesi "
    "koulutuksesta ovat tärkeitä. Kerro meille, missä olemme onnistuneet ja "
    "mitä voisimme tehdä vielä paremmin.</p><p>Toivomme, että käyttäisit noin "
    "10-15 minuuttia aikaa tähän kyselyyn vastaamiseen.</p><p>Palaute annetaan "
    "nimettömänä. Vastauksiasi käytetään ammatillisen koulutuksen "
    "kehittämiseen.</p><p>Kiitos, että vastaat!</p><p>"
    "<a href=\"https://kysely.linkki/123?t=e\">"
    "https://kysely.linkki/123?t=e</a></p><p>Jos linkki ei avaudu, kopioi "
    "linkki selaimesi osoiteriville.</p><p>Terveisin oppilaitoksesi</p><table "
    "border=\"0\" cellpadding=\"0\" cellspacing=\"0\" style=\"width: 100% "
    "!important;\" width=\"100%\"><tr>"
    "<td align=\"left\" height=\"1\" style=\"background-color: #f0f0f0; "
    "border-collapse:collapse; mso-table-lspace: 0pt; mso-table-rspace: 0pt; "
    "mso-line-height-rule: exactly; line-height: 1px;\" valign=\"top\" "
    "width=\"600px\"></td></tr></table><p>Bästa studerande!</p><p>Du har "
    "utvecklats väl och är på väg att slutföra dina yrkesinriktade studier.</p>"
    "<p>Dina erfarenheter av utbildningen är viktiga för oss. Vi vill gärna "
    "höra dina åsikter om hur vi har lyckats och vad vi kunde göra bättre i "
    "fortsättningen.</p><p>Vi hoppas att du tar dig tid att svara på den här "
    "enkäten. Det tar ungefär 10–15 minuter att svara.</p><p>Responsen ges "
    "anonymt. Dina svar används för att utveckla yrkesutbildningen.</p><p>Tack "
    "för att du svarar!</p><p><a href=\"https://kysely.linkki/123?t=e\">"
    "https://kysely.linkki/123?t=e</a></p><p>Om du kan inte öppna länken, "
    "kopiera (och klistra in) länken i webbläsarens adressfält.</p>"
    "<p>Hälsningar, din läroanstalt</p><table border=\"0\" cellpadding=\"0\" "
    "cellspacing=\"0\" style=\"width: 100% !important;\" width=\"100%\"><tr>"
    "<td align=\"left\" height=\"1\" style=\"background-color: #f0f0f0; "
    "border-collapse:collapse; mso-table-lspace: 0pt; mso-table-rspace: 0pt; "
    "mso-line-height-rule: exactly; line-height: 1px;\" valign=\"top\" "
    "width=\"600px\"></td></tr></table><p>Dear student!</p><p>You have "
    "progressed well and are about to complete the vocational studies you set "
    "as your goal.</p><p>Your experiences in education and training matter. "
    "Tell us what we did well and what we could do even better.</p><p>We ask "
    "you to take 10 to 15 minutes to respond to this survey.</p><p>Your "
    "feedback will be collected anonymously. Your responses will be used to "
    "improve vocational education and training.</p><p>Thank you for "
    "responding!</p><p><a href=\"https://kysely.linkki/123?t=e\">"
    "https://kysely.linkki/123?t=e</a></p><p>If you cannot open the link, copy "
    "(and paste) the link onto the address bar of your browser.</p><p>With "
    "best regards, your educational institution</p></div></body></html>"))

(deftest test-amispalaute-html-loppukysely
  (testing "amispalaute-html formats correctly loppukysely"
    (is (= (vp/amispalaute-html
             {:suorituskieli "fi"
              :kyselytyyppi "tutkinnon_suorittaneet"
              :kyselylinkki "https://kysely.linkki/123"})
           mock-amispalaute-html-loppukysely))))

(def mock-amismuistutus-html
  (str
    "<!DOCTYPE html><html lang=\"fi\"><head><meta charset=\"UTF-8\" /></head>"
    "<body><div><p>Olethan muistanut antaa palautetta oppilaitokselle!<br/>"
    "Kom ihåg att ge respons till läroanstalten!<br/>Please remember to give "
    "feedback to the educational institution!</p>"
    "<p><a href=\"https://kysely.linkki/123?t=e\">"
    "https://kysely.linkki/123?t=e</a></p><p>Jos linkki ei avaudu, kopioi "
    "linkki selaimesi osoiteriville.<br/>Om du kan inte öppna länken, "
    "kopiera (och klistra in) länken i webbläsarens adressfält.<br/>If "
    "you cannot open the link, copy (and paste) the link onto the address "
    "bar of your browser.<br/></p>"
    "<table border=\"0\" cellpadding=\"0\" cellspacing=\"0\" "
    "style=\"width: 100% !important;\" width=\"100%\"><tr><td "
    "align=\"left\" height=\"1\" style=\"background-color: #f0f0f0; "
    "border-collapse:collapse; mso-table-lspace: 0pt; "
    "mso-table-rspace: 0pt; mso-line-height-rule: exactly; "
    "line-height: 1px;\" valign=\"top\" width=\"600px\"></td></tr>"
    "</table></div></body></html>"))

(deftest test-amismuistutus-html
  (testing "amismuistutus-html formats correctly"
    (is (= (vp/amismuistutus-html
             {:suorituskieli "fi"
              :kyselylinkki "https://kysely.linkki/123"})
           mock-amismuistutus-html))))

(def mock-tyopaikkaohjaaja-html
  (str
    "<!DOCTYPE html><html lang=\"FI\"><head><meta charset=\"UTF-8\" />"
    "</head><body><div><p><b>Hyvä työpaikkaohjaaja!</b></p><p>Kiitos "
    "koulutussopimus-/oppisopimusopiskelijoiden ohjaamisesta! Tehdään "
    "yhdessä osaajia työelämään.</p><p>Pyydämme vastaamaan tähän "
    "kyselyyn (5 min) yhteistyömme kehittämiseksi. Haluamme kuulla "
    "kokemuksianne ohjaustyöstä ja yhteistyöstä oppilaitoksen kanssa. "
    "Kyselyssä ei arvioida opiskelijaa. Kyselyn voi siirtää työpaikallanne "
    "toiselle henkilölle, jos hän on käytännössä enemmän ohjannut "
    "opiskelijaa. Vastaajan henkilötietoja ei kysytä.</p><p>Palautteenne "
    "on tärkeä, kiitos, että vastaat!</p>"
    "<p><a href=\"https://kysely.linkki/123?t=e\">"
    "https://kysely.linkki/123?t=e</a>"
    "</p><p>Jos linkki ei avaudu, kopioi linkki selaimesi osoiteriville."
    "</p><p>Ystävällisin terveisin,</p><p>Testilaitos, Testilaitos 2</p>"
    "<p>Osoitelähde: Opetushallituksen (OPH) eHOKS-rekisteri</p><table "
    "border=\"0\" cellpadding=\"0\" cellspacing=\"0\" "
    "style=\"width: 100% !important;\" width=\"100%\"><tr><td "
    "align=\"left\" height=\"1\" style=\"background-color: #f0f0f0; "
    "border-collapse:collapse; mso-table-lspace: 0pt; "
    "mso-table-rspace: 0pt; mso-line-height-rule: exactly; "
    "line-height: 1px;\" valign=\"top\" width=\"600px\"></td></tr>"
    "</table><p><b>Bästa arbetsplatshandledare!</b></p><p>Tack för att "
    "Ni handleder studerande på utbildnings-/läroavtal! Tillsammans "
    "skapar vi experter för arbetslivet. </p><p>Vi ber er svara på den "
    "här enkäten (5 min) för att utveckla vårt samarbete. Vi vill lyssna "
    "in er erfarenheter av det dagliga handledningsarbetet och samarbetet "
    "med utbildningsanordnaren. Studerande utvärderas inte i enkäten. "
    "Enkäten kan vidarebefordras till den person som i praktiken har "
    "deltagit mer i handledningen av den studerande. Respondentens "
    "personliga information efterfrågas inte.</p><p>Din respons är "
    "viktig, tack för att du svarar!</p>"
    "<p><a href=\"https://kysely.linkki/123?t=e\">"
    "https://kysely.linkki/123?t=e</a></p><p>Om du kan inte öppna länken, "
    "kopiera (och klistra in) länken i webbläsarens adressfält.</p><p>Med "
    "vänliga hälsningar,</p><p>Testilaitos, Testilaitos 2</p>"
    "<p>Adresskälla: Utbildningsstyrelsens (UBS) ePUK-registret</p><table "
    "border=\"0\" cellpadding=\"0\" cellspacing=\"0\" "
    "style=\"width: 100% !important;\" width=\"100%\"><tr><td "
    "align=\"left\" height=\"1\" style=\"background-color: #f0f0f0; "
    "border-collapse:collapse; mso-table-lspace: 0pt; "
    "mso-table-rspace: 0pt; mso-line-height-rule: exactly; "
    "line-height: 1px;\" valign=\"top\" width=\"600px\"></td></tr>"
    "</table><p><b>Dear workplace instructor!</b></p><p>Thank you for "
    "guiding students with a training agreement/apprenticeship! Let’s "
    "make experts for the workforce together.</p><p>Please respond to "
    "this survey (5 min) to help us work together more effectively. We "
    "would like to hear about your experiences guiding students and "
    "working with the educational institution. The survey does not "
    "assess the student. The survey can be forwarded to another person "
    "at your workplace if he or she has guided the student more. "
    "Respondent´s personal information is not requested.</p><p>Your "
    "feedback is important, thank you for responding!</p>"
    "<p><a href=\"https://kysely.linkki/123?t=e\">"
    "https://kysely.linkki/123?t=e</a></p><p>If you cannot open the link, "
    "copy (and paste) the link onto the address bar of your browser.</p>"
    "<p>With best regards,</p><p>Testilaitos, Testilaitos 2</p><p>Address "
    "source: Opetushallituksen (OPH) eHOKS-register"
    "</p></div></body></html>"))

(deftest test-tyopaikkaohjaaja-html
  (testing "tyopaikkaohjaaja-html formats correctly"
    (is (= (vp/tyopaikkaohjaaja-html
             {:kyselylinkki "https://kysely.linkki/123"}
             [{:fi "Testilaitos"}
              {:fi "Testilaitos 2"}])
           mock-tyopaikkaohjaaja-html))))

(def mock-tyopaikkaohjaaja-muistutus-html
  (str
    "<!DOCTYPE html><html lang=\"FI\"><head><meta charset=\"UTF-8\" /></head>"
    "<body><div><p>Olethan muistanut antaa palautetta oppilaitokselle!<br/>Kom "
    "ihåg att ge respons till läroanstalten!<br/>Please remember to give "
    "feedback to the educational institution!</p><p><a "
    "href=\"https://kysely.linkki/123?t=e\">https://kysely.linkki/123?t=e</a>"
    "</p><p>Jos linkki ei avaudu, kopioi linkki selaimesi osoiteriville.<br/>"
    "Om du kan inte öppna länken, kopiera (och klistra in) länken i "
    "webbläsarens adressfält.<br/>If you cannot open the link, copy (and "
    "paste) the link onto the address bar of your browser.<br/></p><p>Kiitos, "
    "että vastaat - Tack för att du svarar – Thank you for responding!</p>"
    "<p>Testilaitos, Testilaitos 2</p><p><b>Hyvä työpaikkaohjaaja!</b></p>"
    "<p>Kiitos koulutussopimus-/oppisopimusopiskelijoiden ohjaamisesta! "
    "Tehdään yhdessä osaajia työelämään.</p><p>Pyydämme vastaamaan tähän "
    "kyselyyn (5 min) yhteistyömme kehittämiseksi. Haluamme kuulla "
    "kokemuksianne ohjaustyöstä ja yhteistyöstä oppilaitoksen kanssa. "
    "Kyselyssä ei arvioida opiskelijaa. Kyselyn voi siirtää työpaikallanne "
    "toiselle henkilölle, jos hän on käytännössä enemmän ohjannut opiskelijaa. "
    "Vastaajan henkilötietoja ei kysytä.</p><p>Palautteenne on tärkeä, kiitos, "
    "että vastaat!</p><p><a href=\"https://kysely.linkki/123?t=e\">"
    "https://kysely.linkki/123?t=e</a></p><p>Jos linkki ei avaudu, kopioi "
    "linkki selaimesi osoiteriville.</p><p>Ystävällisin terveisin,</p>"
    "<p>Testilaitos, Testilaitos 2</p><p>Osoitelähde: Opetushallituksen (OPH) "
    "eHOKS-rekisteri</p><table border=\"0\" cellpadding=\"0\" "
    "cellspacing=\"0\" style=\"width: 100% !important;\" width=\"100%\"><tr>"
    "<td align=\"left\" height=\"1\" style=\"background-color: #f0f0f0; "
    "border-collapse:collapse; mso-table-lspace: 0pt; mso-table-rspace: 0pt; "
    "mso-line-height-rule: exactly; line-height: 1px;\" valign=\"top\" "
    "width=\"600px\"></td></tr></table><p><b>Bästa arbetsplatshandledare!</b>"
    "</p><p>Tack för att Ni handleder studerande på utbildnings-/läroavtal! "
    "Tillsammans skapar vi experter för arbetslivet. </p><p>Vi ber er svara på "
    "den här enkäten (5 min) för att utveckla vårt samarbete. Vi vill lyssna "
    "in er erfarenheter av det dagliga handledningsarbetet och samarbetet med "
    "utbildningsanordnaren. Studerande utvärderas inte i enkäten. Enkäten kan "
    "vidarebefordras till den person som i praktiken har deltagit mer i "
    "handledningen av den studerande. Respondentens personliga information "
    "efterfrågas inte.</p><p>Din respons är viktig, tack för att du svarar!</p>"
    "<p><a href=\"https://kysely.linkki/123?t=e\">"
    "https://kysely.linkki/123?t=e</a></p><p>Om du kan inte öppna länken, "
    "kopiera (och klistra in) länken i webbläsarens adressfält.</p><p>Med "
    "vänliga hälsningar,</p><p>Testilaitos, Testilaitos 2</p><p>Adresskälla: "
    "Utbildningsstyrelsens (UBS) ePUK-registret</p><table border=\"0\" "
    "cellpadding=\"0\" cellspacing=\"0\" style=\"width: 100% !important;\" "
    "width=\"100%\"><tr><td align=\"left\" height=\"1\" "
    "style=\"background-color: #f0f0f0; border-collapse:collapse; "
    "mso-table-lspace: 0pt; mso-table-rspace: 0pt; "
    "mso-line-height-rule: exactly; line-height: 1px;\" valign=\"top\" "
    "width=\"600px\"></td></tr></table><p><b>Dear workplace instructor!</b></p>"
    "<p>Thank you for guiding students with a training "
    "agreement/apprenticeship! Let’s make experts for the workforce together."
    "</p><p>Please answer this survey (5 min) to help us work together more "
    "effectively. We would like to hear about your experiences guiding "
    "students and working with the educational institution. The survey does "
    "not assess the student. The survey can be forwarded to another person at "
    "your workplace if he or she has guided the student more. Respondent´s "
    "personal information is not requested.</p><p>Your feedback is important, "
    "thank you for responding!</p><p><a href=\"https://kysely.linkki/123?t=e\">"
    "https://kysely.linkki/123?t=e</a></p><p>If you cannot open the link, copy "
    "(and paste) the link onto the address bar of your browser.</p><p>With "
    "best regards,</p><p>Testilaitos, Testilaitos 2</p><p>Address source: "
    "Opetushallituksen (OPH) eHOKS-register</p></div></body></html>"))

(deftest test-tyopaikkaohjaaja-muistutus-html
  (testing "tyopaikkaohjaaja-muistutus-html formats correctly"
    (is (= (vp/tyopaikkaohjaaja-muistutus-html
             {:kyselylinkki "https://kysely.linkki/123"}
             [{:fi "Testilaitos"}
              {:fi "Testilaitos 2"}])
           mock-tyopaikkaohjaaja-muistutus-html))))