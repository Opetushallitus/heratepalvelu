# AWS vinkit

## Testaaminen AWS-työkaluilla

AWS-selainkäyttöliittymän Lambda-työkalujen kautta pääsee tarkastelemaan
funktioiden lokeja, XRay-profilointia ja käynnistämään lambdan testidatalla.
Testidatalla lambdojen käynnistäminen onnistuu myös komentorivillä
sls-komennoilla.

Resurssit (Lambdat, DynamoDB-taulut ja SQS-jonot) on nimetty kaavalla 
`<stage>-heratepalvelu-<resurssin nimi>`

Testidatassa kannattaa kiinnittää huomiota seuraaviin tekijöihin
* Onko organisaatio lisätty organisaatioWhitelist-tauluun?
* Onko opiskeluoikeuteen liitetty HOKS?
* Onko oppija-oid, organisaatio-oid, kyselytyyppi & laskentakausi
yhdistelmäavain uniikki?
* Onko organisaatiolle kyselyä Arvon testiympäristössä?
* Onko opiskeluoikeus oikeaa tyyppiä? (ammatillinentutkinto tai
ammatillinentutkintoosittainen)
* Joskus voi olla myös tarpeen muuttaa edellisen opiskeluoikeuksien
muutostarkistuksen aikaa aikaisemmaksi (metadata-taulusta key
"opiskeluoikeus-last-checked")

Testaaminen onnistuu myös luomalla uuden HOKSin QA:lla eHoks-palveluun
(alkukysely) tai käymällä Koski-käyttöliittymästä lisäämässä vahvistuspäivämäärä
jollekin opiskeluoikeudelle, jolle on luotu HOKS (loppukysely). Lähetettävät
sähköpostit ovat tarkasteltavissa Viestintäpalvelun fakemailerissa.


## Virhetilanteet

Virhetilanteista löytää lisätietoa muutamasta eri paikasta. Pitkäaikaisempaa
dataa löytyy lambdan "Monitoring" -välilehdeltä, kuten myös linkki Cloudwatch
log-grouppiin. Yksittäisten lambda-ajojen tietoja voi tarkastella X-Ray
-traceista (linkki löytyy "Configuration" -välilehden alaosasta).

Suurin osa virheistä johtaa lambdan uudelleen ajoon, ja useimmissa tapauksissa
ongelma ratkeaa itsekseen. Jos jokin näyttää menevän pahasti pieleen, eikä näytä
tokenevan omin avuin, voi lambdan automaattiset ajot estää disabloimalla
lambda-ajon käynnistävän eventin. Disablointi nappula löytyy "Configuration"
-välilehdeltä "Designer" -laatikosta eventin lähdettä klikkaamalla (esim. SQS
eHOKSHerateHandlerissa tai CloudWatch Events ajastetuilla lambdoilla). Eventin
lähteen klikkaamisen jälkeen paljastuu lähteen tarkempi konfiguraatio, jonka
oikeassa reunassa "Enabled/Disabled" valinta. Vaihda tila sopivaan ja tallenna
muutokset.

HUOM!! Lambdan disablointi on aina viimeinen keino ja sopii vain tilanteisiin,
joissa se on ehdoton pakko!
