# AMIS-osio

AMIS-osio käsittelee niiden kyselyjen lähettämistä, jotka lähetetään opiskelijan
opintojen alussa ja lopussa. Kyselyn lähettämisen jälkeen opiskelijalla on
tietty määrä aikaa vastata (yleensä kaksi kuukautta), jonka jälkeen kysely
suljetaan ja vastauksia ei enää oteta vastaan.

## Rakenne

Kun uuden opiskelijan HOKS tallennetaan eHOKS-palveluun, SQS-viesti ("heräte")
luodaan ja lähetetään herätepalveluun. Heräte otetaan vastaan
AMISherateHandlerissa, joka tarkistaa herätteen oikeamuotoisuuden ja luo sille
rivin tietokannan herätetauluun, jos kaikki testit läpäistään. Jos testejä ei
läpäistä tai herätteellä ei ole voimassaolevaa opiskeluoikeutta, herätettä ei
tallenneta.

Sähköpostien lähetystä käsitellään neljässä eri funktiossa. Ensimmäinen niistä,
AMISherateEmailHandler, hakee tietokannasta herätteitä, joiden tapauksessa
sähköpostia ei ole vielä lähetetty ja aloittaa sähköpostien lähettämisen erillisen
viestintäpalvelun kautta. Viestien tilaa tarkastellaan lähettämisen jälkeen 
säännöllisesti viestintäpalvelusta EmailStatusHandler-funktiossa. Kun 
viestintäpalvelu palauttaa viestille tilan, se päivitetään herätteelle tietokantaan. 
EmailStatusHandler käsittelee sekä ensimmäisten sähköpostien että muistutusviestien 
(ks. alas) tiloja.

AMISMuistutusHandler toimii pääpiirteittäin samalla tavalla kuin AMISherateEmailHandler.
Ensimmäinen muistutusviesti lähetetään viiden päivän jälkeen ja toinen kymmenen
päivän jälkeen, jos kyselyyn ei ole vastattu ja vastausaikaa on vielä jäljellä.
AMISEmailResendHandler puolestaan uudelleenlähettää ensimmäisen kyselyn, 
kun eHOKS-palvelu lähettää sille uudelleenlähetysviestin SQS:n kautta.

Päättökyselyt, jotka luodaan kun opiskelija valmistuu, käsitellään samoin.

## Ongelmatilanteiden tutkiminen

### Hoksille 12345 ei ole lähteny aloitus- tai päättökyselyä

Tämän kaltaiset selvitystehtävät ovat yleisiä tässä projektissa. Alla on 
listattu paikkoja, joista voi selvitellä herätteen tai kyselyn tilaa.

#### Ehoks

- Ehoksin ``hoksit`` -taulusta näet onko Hoks ylipäätään muodostunut.
- Ehoksin ``amisherate_kasittelytilat``-taulusta näet onko heräte 
  käsitelty herätepalvelun päässä. Jos kentissä ``aloitusherate_kasitelty`` tai 
  ``paattoherate_kasitelty`` on arvo ``true``, heräte on onnistuneesti 
  käsitelty herätepalvelun päässä. HUOM: kentät voivat olla ``true`` myös 
  siitä syystä, että kyseinen hoks on TUVA-hoks. Näissä tapauksissa 
  palautteita ei kerätä ja käsittelytilat merkataan käsitellyiksi heti 
  hoksin luonnin yhteydessä.
- CloudWatchista löytyy Ehoksin logit, joista voi etsiä logituksia 
  AMIS-herätteiden lähetyksistä SQS-jonoon. Kyseisiä logirivejä voi hakea 
  hoks-id:llä, opiskeluoikeuden oidilla tai oppija-oidilla.

#### Herätepalvelu

- 123

Näissä tilanteissa kannattaa lähteä liikkeelle Ehoksista ja varmistua siitä, 
että Hoks on muodostunut tietokantaan normaalisti. Voit samalla tarkastaa 
Ehoksin tietokannan taulusta ``amisherate_kasittelytilat``, onko löytyykö 
hoksin id:llä rivi ja mitkä ovat kenttien ``aloitusherate_kasitelty`` ja 
``paattoherate_kasitelty`` arvot. Jos herätteen arvo on ``false``, 
herätepalvelu ei ole käsitellyt sitä onnistuneesti.