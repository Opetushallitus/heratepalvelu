# External palvelut

## Arvo

Arvo luo linkkejä opiskelijakyselyihin ja kerää niihin annetut vastaukset.
Opiskelijoiden vastauksia ei käsitellä herätepalvelussa, vaan Arvosta haetaan
vain linkki itse ja siihen liittyvät metatiedot (esim. vastausajan ensimmäinen
ja viimeinen päivämäärät ja vastattu -tila).

Tiedostossa `src/oph/heratepalvelu/external/arvo.clj` on useita apufunktioita,
joilla haetaan linkkejä, haetaan tai päivitetään niiden metatiedot, tai
poistetaan ne; tai joilla näiden funktioiden argumentit saadaan oikeaan muotoon.


## eHOKS

eHOKS käsittelee opiskelijoiden HOKSeja, joihin kuuluu muun muassa osaamisen
hankkimistapoja, joista TEP-jaksot luodaan. Kyselylinkit lähetetään myös
eHOKSiin sen jälkeen, että ne on luotu herätepalvelussa. Tiedosto
`src/oph/heratepalvelu/external/ehoks.clj` sisältää funktioita, joilla haetaan
tietoja HOKSeista ja linkkitiedot takaisin palveluun.


## Elisa

Elisa käsittelee SMS-viestien todellista lähetystä. Tiedostossa
`src/oph/heratepalvelu/external/elisa.clj` on funktioita, joilla SMS-viestien
tekstit luodaan ja viestit lähetetään palvelun kautta.


## Koski

Koski käsittelee muun muassa opiskeluoikeuksia. Tiedostosta
`src/oph/heratepalvelu/external/koski.clj` löytyy funktioita, joilla
opiskeluoikeuksia haetaan eräiden kriteerien perusteella.


## Organisaatio

Organisaatiopalvelu tarjoaa tietoja organisaatioista (esim. korkeakouluista).
Tiedostosta `src/oph/heratepalvelu/external/organisaatio.clj` löytyy funktioita,
joilla nämä tiedot haetaan.


## Viestintäpalvelu

Viestintäpalvelu käsittelee sähköpostiviestien todellista lähetystä. Tiedostossa
`src/oph/heratepalvelu/external/viestintapalvelu.clj` on funktioita, joilla
sähköpostiviestien tekstit luodaan, viestit lähetetään viestintäpalveluun, ja
viestien tilat tarkistetaan palvelusta.


## Utilities

### AWS SQS

Sisältää funktion, jolla viestit lähetetään SQS:iin.


### AWS SSM

Sisältää funktion, jolla salaiset arvot haeteen SSM:istä.


### AWS X-Ray

Sisältää funktion, jolla requestit kääritään AWS X-Rayiin.


### Cas Client

Sisältää funktioita, joilla tehdään Cas-autentikoituja requestejä.


### HTTP Client

Sisältää wrappereita clj-http -clientin ympäri.
