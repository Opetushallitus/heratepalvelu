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
listattu paikkoja, joista voi selvitellä herätteen tilaa.

#### Ehoks

- Ehoksin ``hoksit`` -taulusta näet onko Hoks muodostunut onnistuneesti.
- Ehoksin ``amisherate_kasittelytilat``-taulusta näet onko heräte 
  käsitelty herätepalvelussa. Jos kentissä ``aloitusherate_kasitelty`` tai 
  ``paattoherate_kasitelty`` on arvo ``true``, heräte on onnistuneesti 
  käsitelty herätepalvelussa. HUOM: kentät voivat olla ``true`` myös 
  siitä syystä, että kyseinen hoks on TUVA-hoks. Näissä tapauksissa 
  palautteita ei kerätä ja käsittelytilat merkataan käsitellyiksi heti 
  hoksin luonnin yhteydessä.
- CloudWatchista löytyy Ehoksin logit, joista voi etsiä logeja 
  AMIS-herätteiden lähetyksistä SQS-jonoon. Hakusanana voi käyttää 
  hoks-id:tä, oppijanumeroa tai opiskeluoikeuden oidia.

#### Herätepalvelu

- AMIS-herätteiden käsittelyn logitukset löytyvät 
  ``AMISHerateHandler``-funktion CloudWatch-logeilta.
- Herätteet löytyvät DynamoDB:stä AMISHerateTable:sta. Haku kannattaa tehdä 
  querynä hoks-id:llä indeksistä. Taulun kenttä ``lahetystila`` kertoo 
  sähköpostin lähetyksen tilan ja sms-lahetystila kertoo sms:n lähetyksen 
  tilan. Jos tila on jokin muu kuin lahetetty tai vastattu, niin tutki syy 
  sille koodista.
