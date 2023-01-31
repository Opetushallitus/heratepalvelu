# Muut tiedot ja ohjeet

## Vanhojen herätteiden ja nippujen arkistointi

Ne AMIS ja TEP -taulujen tietueet, jotka kuuluvat edeltävään rahoituskauteen, 
arkistoidaan jokaisen rahoituskauden päätteeksi. Tähän käytetään funktioita
archiveHerateTable (`src/oph/heratepalvelu/amis/archiveHerateTable.clj`),
archiveJaksoTable (`src/oph/heratepalvelu/tep/archiveJaksoTable.clj`) ja
archiveNippuTable (`src/oph/heratepalvelu/tep/archiveNippuTable.clj`). Kun
lisäät uuden rahoituskauden arkistointiin, lisää uusi rivi kyseessä olevan
handlerin bodyyn, määrittele uusi arkistotaulu `cdk/lib/amis.ts` tai
`cdk/lib/tep.ts` -tiedostossa, ja anna referenssi tähän tauluun kyseessä olevaan
lambdafunktioon. Lambdafunktioiden cdk-määrittelyt on yleensä kommentoitu pois,
kun niitä ei tarvita. Ennen arkistointia nämä kommentoinnit pitää poistaa ja 
asentaa palvelu uudestaan. Arkistoinnin jälkeen kommentoi funktiot jälleen 
pois käytöstä ja tee asennus. Tämä tehdään siksi, että vältetään turhia 
kustannuksia funktioista, joita ei käytetä kuin kerran vuodessa. HUOM: Ole 
tarkkana ettet kommentoi pois luomaasi uutta arkistotaulua.

Työpaikkakyselyn nipputaulu arkistoidaan kaksi kertaa vuodessa, mutta prosessi
on käytännössä sama. Arkistointifunktion koodi löytyy tiedostosta
`src/oph/heratepalvelu/tpk/archiveTpkNippuTable.clj`.

Arkistotauluja säilytetään vähintään 5 vuotta. Jos kehittäjänä huomaat, ettei 
arkistointia ole nostettu weeklyissä keskusteluihin rahoituskauden 
vaihteessa (maaliskuussa ja syyskuussa — rahoituskaudet kestävät heinäkuusta
seuraavan vuoden kesäkuuhun, ja työpaikkakyselyn tiedonkeruukaudet kestävät
tammikuusta kesäkuuhun ja heinäkuusta joulukuuhun, mutta arkistoinnin voi tehdä
vasta, kun kyselyjen vastausaika on päättynyt), tästä on hyvä mainita OPH:n 
edustajille. Myös arkistotaulujen poistoista voi olla hyvä kehittäjänä olla 
perillä ja mainita asiasta jos se tulee ajankohtaiseksi. Älä tee arkistointia 
äläkä poista arkistoitua taulua ilman OPH:n edustajien suostumusta.

Kaikki arkistointifunktiot täytyy ajaa käsin AWS Consolesta. Mene 
Lambdafunktion Test-välilehdelle ja käynnistä funktio painamalla 
Test-painiketta. Arkistoitavien tietueiden määrästä riippuen voit joutua 
ajamaan funktioita useita kertoja. Ei ole väliä, missä järjestyksessä 
funktiot ajetaan, kunhan niitä ei ajeta liian aikaisin.

## Oppijanumeron päivittäminen eHOKS:iin Oppijanumerorekisteristä

Herätepalvelun AWS-infrastruktuuria käytetään jonkin verran hyödyksi eHOKS:iin 
liittyvissä automatisoiduissa ratkaisuissa. Yksi näistä on oppijan tietojen 
muutosten seuranta Oppijanumerorekisterissä ja sen myötä tehtävät tietojen 
päivitykset eHOKS:iin.

Tämä ratkaisu on toteutettu AMIS-osion puolella ja määrittelyt löytyvät 
`/lib/amis.ts` -tiedostosta.

Muutoksia varten on luotu oma SQS-jono joka on rekisteröity seuraamaan 
Oppijanumerorekisterin SNS-topic:a. SQS-jonoon saapuvat viestit triggeröivät 
puolestaan funktion `/util/ONRhenkilomodify.clj`, joka lähettää tiedot 
muutoksista eHOKS:n rajapintaan. Funktiossa tapahtuu aika ajoin virhe, joka 
suurella todennäköisyydellä johtuu siitä, ettei eHOKS vastaa tarpeeksi 
nopeasti kutsuun. Yleensä nämä käsittelyt onnistuvat automaattisessa 
uudelleenkäsittelyssä, mutta joskus ne päätyvät DLQ-jonoon, joka täytyy 
manuaalisesti tyhjentää. Tätä varten on olemassa `util/ONRDLQresendHandler.
clj`, joka tyhjennetään samaan tapaan kuin palvelun muutkin DLQ-jonot, eli 
aktivoidaan Lambdan triggeri AWS Consolesta ja odotetaan, että DLQ on tyhjä. 
Sen jälkeen trigger disabloidaan.