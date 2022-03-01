# AMIS-palvelu

AMIS-palvelu käsittelee noiden kyselyjen lähettämistä, jotka lähetetään
opiskelijan opiskelu-uran alussa ja lopussa. Kyselyn lähettämisen jälkeen
opiskelijalla on tietty määrä aikaa vastata (yleensä kaksi kuukautta), jonka
jälkeen kysely suljetaan ja vastauksia ei enää oteta vastaan.

## Rakenne

Kun uuden opiskelijan HOKS tallennetaan eHOKS-palveluun, SQS-viesti ("heräte")
luodaan ja lähetetään herätepalveluun. Heräte otetaan vastaan
AMISherateHandlerissa, joka tarkistaa herätteen oikeamuotoisuuden ja luo sille
rivin tietokannan herätetauluun, jos kaikki testit läpäistään. Jos testejä ei
läpäistä tai herätteellä ei ole voimassaolevaa opiskeluoikeutta, herätettä ei
tallenneta.

Sähköpostien lähetystä käsittellään neljässä eri funktiossa. Ensimmäinen niistä,
AMISherateEmailHandler, hakee tietokannasta herätteitä, joilta sähköpostia ei
ole vielä lähetetty, ja aloittaa sähköpostin lähetyksen erillisen
viestintäpalvelun kautta. Toinen funktio, EmailStatusHandler, tarkistaa
säännöllisesti viestintäpalvelussa olevien sähköpostien tilaa ja päivittää
kyseessä olevan tietokantarivin, kun viesti on lähtenyt tai virhe on tapahtunut
lähetyksessä. EmailStatusHandler käsittelee sekä ensimmäisten sähköpostien että
muistutusviestien (ks. alas) tiloja.

AMISMuistutusHandler toimii käytännössä samalla tavalla kuin
AMISherateEmailHandler paitsi se, että se aloittaa muistutusviestin lähetyksen
silloin, kun ensimmäisessä viestissä lähetettyyn kyselyyn ei ole vastattu 5 tai
10 päivää sen ensimmäisen lähetyksen jälkeen, mutta vastaajalla olisi vielä
aikaa vastata. AMISEmailResendHandler puolestaan lähettää ensimmäisen
sähköpostin uudestaan, kun eHOKS-palvelu lähettää sille uudelleenlähetysviestin
SQS:n kautta.


