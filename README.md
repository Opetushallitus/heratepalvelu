# Herätepalvelu

Herätepalvelu Arvo-kyselylinkkien lähetyksen automatisointiin.


## Toiminta

### Aloituskyselyt

Aloituskyselyjen käsittely alkaa uuden HOKSin tallentamisesta eHOKS-palveluun.
eHOKS lähettää herätteen SQS-jonoon, joka laukaisee eHOKSherateHandlerin.
Kyselyn vastausajan aloituspäivänä käytetään HOKSin ensikertaisen hyväksymisen
päivämäärää. Käsitelty heräte tallennetaan herateTable DynamoDB-tauluun.


### Lopetuskyselyt

Lopetuskyselyt käsitellään hakemalla ajastetusti Koski-palvelusta muuttuneet
opiskeluoikeudet. Kyselyn vastausajan aloituspäivänä käytetään opiskeluoikeuden
päätason suorituksen vahvistus päivämäärää. Käsitelty heräte tallennetaan
herateTable DynamoDB-tauluun.


### Sähköpostien lähettäminen

Sähköpostit lähetetään hakemalla ajastetusti uudet tietueet
herateTable-taulusta, jotka lähetetään Viestintäpalveluun. Lähetettyjen
viestien tila ja seuranta-id tallennetaan herateTable-tauluun.


### Organisaatioden käyttöönoton ylläpitäminen

Organisaatioiden käyttöönoton ajankohdat tallennetaan käsin
organisaatioWhitelist-tauluun. Katso esimerkkiä olemassa olevista tietueista.


## Kehittäminen

### Kehitysympäristö

Palvelu on kirjoitettu Clojurella, ja projektinhallintaan käytetään Leiningeniä.
Deploy-työkaluna käytetään AWS CDK:ta. AWS profiilien hallintaan aws-vault
-työkalun käyttö on suositeltavaa.

`npm install -g aws-cdk`

`brew cask install aws-vault`
(muokkaa käyttämääsi paketinhallintaan sopivaksi)

Stageja ovat samat ympäristönimet kuin muissakin palveluissa (esim. 'pallero').

Deploy cdk:lla tapahtuu seuraavanlaisin komennoin:

Paketin buildaaminen

`lein uberjar`

Asennus testiympäristöihin

`./deploy.sh pallero <amis/tep/tpk>`

Asennus tuotantoon

`./deploy.sh sade <amis/tep/tpk>`

Yksikkötestit voit ajaa komennolla

`lein test`

Ympäristömuuttujat löytyvät SSM Parameter Storesta ja Lambda ajojen välistä
tilaa voi tarvittaessa tallentaa key-value pareina metadata-tauluun.


## Dokumentaatio

Enemmän dokumentaatiota löytyy kansiosta `doc/`.
