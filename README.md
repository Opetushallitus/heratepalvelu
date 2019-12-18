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
käytetään AWS CDK:ta. AWS profiilien hallintaan aws-vault -työkalun käyttö on suositeltavaa.

`npm install -g aws-cdk`

`brew install cask aws-vault`
(muokkaa käyttämääsi paketinhallintaan sopivaksi)

Stageja ovat samat ympäristönimet kuin muissakin palveluissa (esim. 'pallero').

Deploy cdk:lla tapahtuu seuraavanlaisin komennoin:

Paketin buildaaminen

`lein uberjar`

Deploy

`cd cdk`

Testiympäristöihin

`aws-vault exec oph-dev -- cdk deploy pallero-services-heratepalvelu`

Tuotantoon

`aws-vault exec oph-prod -- cdk deploy sade-services-heratepalvelu`

Yksikkötestit voit ajaa komennolla

`lein test`

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

### Virhetilanteet

Virhetilanteista löytää lisätietoa muutamasta eri paikasta. 
Pitkäaikaisempaa dataa löytyy lambdan "Monitoring" -välilehdeltä, 
kuten myös linkki Cloudwatch log-grouppiin. 
Yksittäisten lambda-ajojen tietoja voi tarkastella X-Ray -traceista 
(linkki löytyy "Configuration" -välilehden alaosasta).

Suurin osa virheistä johtaa lambdan uudelleen ajoon, 
ja useimmissa tapauksissa ongelma ratkeaa itsekseen. 
Jos jokin näyttää menevän pahasti pieleen, eikä näytä 
tokenevan omin avuin, voi lambdan automaattiset ajot estää 
disabloimalla lambda-ajon käynnistävän eventin.
Disablointi nappula löytyy "Configuration" -välilehdeltä "Designer" -laatikosta 
eventin lähdettä klikkaamalla (esim. SQS eHOKSHerateHandlerissa tai 
CloudWatch Events ajastetuilla lambdoilla). Eventin lähteen klikkaamisen 
jälkeen paljastuu lähteen tarkempi konfiguraatio, jonka oikeassa reunassa 
"Enabled/Disabled" valinta. Vaihda tila sopivaan ja tallenna muutokset.

HUOM!! Lambdan disablointi on aina viimeinen keino ja sopii vain
tilanteisiin, joissa se on ehdoton pakko!