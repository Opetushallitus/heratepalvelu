# External palvelut

## Arvo

Arvo luo linkkejä opiskelijakyselyihin ja kerää niihin annetut vastaukset.
Opiskelijoiden vastauksia ei käsitellä herätepalvelussa, vaan Arvosta haetaan
vain kyselylinkki ja siihen liittyvät metatiedot (esim. vastausajan ensimmäinen
ja viimeinen päivämäärät ja vastattu -tila).

Tiedostossa `src/oph/heratepalvelu/external/arvo.clj` on useita apufunktioita,
joilla haetaan linkkejä, haetaan tai päivitetään niiden metatiedot, tai
poistetaan ne; tai joilla näiden funktioiden argumentit saadaan oikeaan muotoon.

Jos kutsu Arvoon palauttaa 404-virheen viestillä `Ei kyselykertaa annetuille
tiedoille`, kannattaa pyytää apua Arvo-tiimiltä. Tämä ei viittaa
ongelmaan herätepalvelussa, vaan tarkoittaa, että Arvo-tiimi ei ole vielä
avannut palveluaan tuon aikavälin kyselyjen luomiseen.


## eHOKS

eHOKS käsittelee opiskelijoiden HOKSeja, joihin kuuluu muun muassa osaamisen
hankkimistapoja, joista TEP-jaksot luodaan. Kyselylinkit lähetetään myös
eHOKSiin sen jälkeen, että ne on luotu herätepalvelussa. Tiedosto
`src/oph/heratepalvelu/external/ehoks.clj` sisältää funktioita, joilla haetaan
tietoja HOKSeista ja joilla palautetaan tietoja kyselyistä takaisin eHOKS:n.

Herätepalvelun kehitystyössä monesti muutokset ja uusien toiminnallisuuksien 
lisäykset vaativat muutoksia sekä Herätepalveluun että eHOKS-palveluun.


## Elisa

Elisa käsittelee SMS-viestien todellista lähetystä. Tiedostossa
`src/oph/heratepalvelu/external/elisa.clj` on funktioita, joilla SMS-viestien
tekstit luodaan ja viestit lähetetään palvelun kautta.

Ulkoisen palvelun nimi on Elisa Dialogi (vuonna 2024) ja sen
API-dokumentaatio on
[täällä](https://docs.dialogi.elisa.fi/docs/dialogi/send-sms).

Lähetettyjä viestejä ja käyttöä voi seurata [Dialogin
hallintokäyttöliittymästä](https://viestipalvelu.elisa.fi/login) ja
siihen saa tunnukset OPH:n virkailijoilta.


## Koski

Koski käsittelee muun muassa opiskeluoikeuksia. Tiedostosta
`src/oph/heratepalvelu/external/koski.clj` löytyy funktioita, joilla
opiskeluoikeuksia haetaan eri kriteerien perusteella.


## Organisaatio

Organisaatiopalvelu tarjoaa tietoja organisaatioista (esim. korkeakouluista).
Tiedostosta `src/oph/heratepalvelu/external/organisaatio.clj` löytyy funktioita,
joilla nämä tiedot haetaan.


## Viestintäpalvelu

Viestintäpalvelu käsittelee sähköpostiviestien todellista lähetystä. Tiedostossa
`src/oph/heratepalvelu/external/viestintapalvelu.clj` on funktioita, joilla
sähköpostiviestien tekstit luodaan, viestit lähetetään viestintäpalveluun ja
viestien tilat tarkistetaan palvelusta.


## Utilities

### AWS SQS

Sisältää funktion, jolla viestit lähetetään SQS:iin.


### AWS SSM

Sisältää funktion, jolla salatut arvot haeteen SSM:istä.


### AWS X-Ray

Sisältää funktion, jolla requestit kääritään AWS X-Ray:n.


### Cas Client

Sisältää funktioita, joilla tehdään Cas-autentikoituja http-pyyntöjä.


### HTTP Client

Sisältää wrappereita clj-http -clientin ympärille.
