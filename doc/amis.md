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
