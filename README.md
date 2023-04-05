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

Yksikkötestit voit ajaa komennolla

`lein test`

### Asennukset

Deploy-työkaluna käytetään AWS CDK:ta, josta lisää
[cloud-basessa](https://github.com/Opetushallitus/cloud-base/blob/master/docs/infrastructure-management.md#cdk-ja-cdksh).
Herätepalvelulla on oma [cdk.sh](./cdk.sh).

Esivaatimuksena tarvitset myös OPH:n AWS-tilit konfiguroiduksi, helpoin tapa
siihen on seurata [ohjeita
cloud-basessa](https://github.com/Opetushallitus/cloud-base/blob/master/docs/new-developer.md).

Deploy cdk:lla tapahtuu seuraavanlaisin komennoin:

Asennus testiympäristöihin

```shell
$ ./cdk.sh pallero deploy pallero-services-heratepalvelu   # amis
$ ./cdk.sh pallero deploy pallero-services-heratepalvelu-tep
$ ./cdk.sh pallero deploy pallero-services-heratepalvelu-tpk
$ ./cdk.sh pallero deploy pallero-services-heratepalvelu-teprah
```

Asennus tuotantoon

```shell
$ ./cdk.sh sade deploy sade-services-heratepalvelu   # amis
$ ./cdk.sh sade deploy sade-services-heratepalvelu-tep
$ ./cdk.sh sade deploy sade-services-heratepalvelu-tpk
$ ./cdk.sh sade deploy sade-services-heratepalvelu-teprah
```

Ympäristömuuttujat löytyvät SSM Parameter Storesta ja Lambda ajojen välistä
tilaa voi tarvittaessa tallentaa key-value pareina metadata-tauluun.

## Dokumentaatio

Enemmän dokumentaatiota löytyy kansiosta `doc/`.
