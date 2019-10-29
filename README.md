# Herätepalvelu

Herätepalvelu Arvo-kyselylinkkien lähetyksen automatisointiin.

## Toiminta

### Aloituskyselyt

Aloituskyselyjen käsittely alkaa uuden HOKSin tallentamisesta eHOKS-palveluun. eHOKS lähettää
herätteen SQS-jonoon, joka laukaisee eHOKSherateHandlerin. Kyselyn vastausajan aloituspäivänä
käytetään HOKSin ensikertaisen hyväksymisen päivämäärää. Käsitelty heräte tallennetaan 
herateTable DynamoDB-tauluun.

### Lopetuskyselyt

Lopetuskyselyt käsitellään hakemalla ajastetusti Koski-palvelusta muuttuneet opiskeluoikeudet.
Kyselyn vastausajan aloituspäivänä käytetään opiskeluoikeuden päätason suorituksen vahvistus 
päivämäärää. Käsitelty heräte tallennetaan herateTable DynamoDB-tauluun.

### Sähköpostien lähettäminen

Sähköpostit lähetetään hakemalla ajastetusti uudet tietueet herateTable-taulusta, jotka lähetetään
Viestintäpalveluun. Lähetettyjen viestien tila ja seuranta-id tallennetaan herateTable-tauluun.

### Organisaatioden käyttöönoton ylläpitäminen

Organisaatioiden käyttöönoton ajankohdat tallennetaan käsin organisaatioWhitelist-tauluun. 
Katso esimerkkiä olemassa olevista tietueista.

## Kehittäminen

### Kehitysympäristö

Palvelu on kirjoitettu Clojurella, ja projektinhallintaan käytetään Leiningeniä. Deploy-työkaluna
käytetään Serverless Frameworkia.

Serverless Frameworkin saat asennettua

``npm install -g serverless``

jonka jälkeen deployn voit tehdä komennoilla

`lein uberjar`

`sls deploy -s <stage>`

Stageja ovat samat ympäristönimet kuin muissakin palveluissa (esim. 'pallero'), default arvona 'sieni'.

Jos haluat määrittää erikseen käytettävän profiilin, käytä vipua `--aws-profile <profiili>`. Jos serverless valittaa
puuttuvasta profiilista, vaikka se olisi oikein konfiguroitu, niin seuraavan komennon ajaminen ennen deployta pitäisi
korjata se: `export AWS_SDK_LOAD_CONFIG=1`.

Yksikkötestit voit ajaa komennolla

`lein test`

Lisätietoja Serverless-työkalusta löydät [dokumentaatiosta](https://serverless.com/framework/docs/) 
tai komennolla

`sls --help`

Ympäristömuuttujat löytyvät SSM Parameter Storesta ja Lambda ajojen välistä tilaa voi tarvittaessa
tallentaa key-value pareina metadata-tauluun.

### Testaaminen AWS-työkaluilla

AWS-selainkäyttöliittymän Lambda-työkalujen kautta pääsee tarkastelemaan funktioiden lokeja, 
XRay-profilointia ja käynnistämään lambdan testidatalla. Testidatalla lambdojen käynnistäminen
onnistuu myös komentorivillä sls-komennoilla.

Resurssit (Lambdat, DynamoDB-taulut ja SQS-jonot) on nimetty kaavalla 
`<stage>-heratepalvelu-<resurssin nimi>`

Testidatassa kannattaa kiinnittää huomiota seuraaviin tekijöihin
* Onko organisaatio lisätty organisaatioWhitelist-tauluun?
* Onko opiskeluoikeuteen liitetty HOKS?
* Onko oppija-oid, organisaatio-oid, kyselytyyppi & laskentakausi yhdistelmäavain uniikki?
* Onko organisaatiolle kyselyä Arvon testiympäristössä?
* Onko opiskeluoikeus oikeaa tyyppiä? (ammatillinentutkinto tai ammatillinentutkintoosittainen)
* Joskus voi olla myös tarpeen muuttaa edellisen opiskeluoikeuksien muutostarkistuksen aikaa
aikaisemmaksi (metadata-taulusta key "opiskeluoikeus-last-checked")

Testaaminen onnistuu myös luomalla uuden HOKSin QA:lla eHoks-palveluun (alkukysely) tai käymällä 
Koski-käyttöliittymästä lisäämässä vahvistuspäivämäärä jollekin opiskeluoikeudelle, 
jolle on luotu HOKS (loppukysely). Lähetettävät sähköpostit ovat tarkasteltavissa Viestintäpalvelun
fakemailerissa.
